use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
        Path,
    },
    response::IntoResponse,
    routing::get,
    Router,
};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

// アプリ全体の状態
struct AppState {
    // ルーム名 -> そのルーム用の broadcast::Sender
    rooms: RwLock<HashMap<String, broadcast::Sender<String>>>,
}

#[tokio::main]
async fn main() {
    // 最初はルームは空
    let app_state = Arc::new(AppState {
        rooms: RwLock::new(HashMap::new()),
    });

    // ルーティング: /ws/:room_id
    let app = Router::new()
        .route("/ws/:room_id", get(ws_handler))
        .with_state(app_state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 5000));
    let listener = TcpListener::bind(addr).await.unwrap();

    println!("listening on {}", addr);

    axum::serve(listener, app).await.unwrap();
}

// 指定された room_id の Sender を取得（なければ作る）
async fn get_or_create_room_sender(
    state: &Arc<AppState>,
    room_id: &str,
) -> broadcast::Sender<String> {
    // まず read ロックで存在確認
    {
        let rooms = state.rooms.read().await;
        if let Some(sender) = rooms.get(room_id) {
            return sender.clone();
        }
    }

    // なければ write ロックで新しいルームを作成
    let mut rooms = state.rooms.write().await;

    // write ロックまで来た時点で、他のタスクが先に作った可能性もあるので再確認
    if let Some(sender) = rooms.get(room_id) {
        return sender.clone();
    }

    let (tx, _rx) = broadcast::channel(100);
    rooms.insert(room_id.to_string(), tx.clone());
    tx
}

// WebSocketハンドラ: /ws/:room_id
async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(room_id): Path<String>,        // URL から room_id を取得
    State(state): State<Arc<AppState>>, // アプリ状態
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, room_id))
}

// 実際の通信処理
async fn handle_socket(socket: WebSocket, state: Arc<AppState>, room_id: String) {
    // この接続が参加するルームの sender を取得
    let room_sender = get_or_create_room_sender(&state, &room_id).await;

    let (mut sender, mut receiver) = socket.split();

    // このルームのメッセージだけを受け取る rx
    let mut rx = room_sender.subscribe();

    // 送信用タスク: ルームの broadcast から受け取って、このクライアントに送信
    let send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // 受信用ループ: クライアントからのメッセージをこのルームに broadcast
    while let Some(msg_result) = receiver.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                // 同じ room_id に居るクライアントにしか届かない
                let _ = room_sender.send(text);
            }
            Ok(Message::Close(_)) => {
                break;
            }
            Ok(_) => {
                // Binary や Ping などは今は無視
            }
            Err(_) => {
                break;
            }
        }
    }

    send_task.abort();
}