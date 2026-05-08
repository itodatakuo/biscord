use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path,
        State,
    },
    response::IntoResponse,
    routing::{get,post},
    Router,
};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};

use sqlx::PgPool;
use bcrypt::{hash, verify, DEFAULT_COST};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

// アプリ全体の状態
struct AppState {
    // ルーム名 -> そのルーム用の broadcast::Sender
    rooms: RwLock<HashMap<String, broadcast::Sender<String>>>,

    sessions: RwLock<HashMap<String,String>>,

    db: PgPool,
}
//リクエスト/レスポンス
use serde::{Deserialize,Serialize};

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResponse{
    token:String,
}

use axum::{Json,http::StatusCode};
use uuid::Uuid;

use axum::extract::Query;

#[derive(Deserialize)]
struct WsQuery {
    token:String,
}

#[derive(Deserialize)]
struct RegisterRequest {
    username: String,
    password: String,
}

#[tokio::main]
async fn main() {
    // 最初はルームは空

    let db = PgPool::connect(
        "postgres://postgres:takuo2525@localhost:5432/chat"
    ) 
    .await
    .unwrap();

    let app_state = Arc::new(AppState {
        rooms: RwLock::new(HashMap::new()),
        sessions:RwLock::new(HashMap::new()),
        db,
    });

    // ルーティング: /ws/:room_id
    let app = Router::new()
        .route("/ws/{room_id}", get(ws_handler))
        .route("/login",post(login_handler))
        .route("/register", post(register_handler))
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

//wsハンドラ
async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(room_id): Path<String>,
    Query(query): Query<WsQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let username = {
        let sessions = state.sessions.read().await;
        sessions.get(&query.token).cloned()
    };

    if let Some(username) = username {
        let state = Arc::clone(&state);
        ws.on_upgrade(move |socket| {
            handle_socket(socket, state, room_id, username)
        })
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

//ログインハンドラ
use sqlx::Row;

async fn login_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, StatusCode> {
    let record = sqlx::query(
        "SELECT password_hash FROM users WHERE username = $1"
    )
    .bind(&req.username)
    .fetch_optional(&state.db)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let Some(row) = record else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    // ✅ フィールドアクセスは一切しない
    let password_hash: String = row
        .try_get("password_hash")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if !verify(&req.password, &password_hash).unwrap() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token = Uuid::new_v4().to_string();

    state.sessions.write().await.insert(token.clone(), req.username);

    Ok(Json(LoginResponse { token }))
}

// レジスタハンドラ
async fn register_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<StatusCode, StatusCode> {
    let password_hash = hash(&req.password,DEFAULT_COST)
    .map_err(|_|StatusCode::INTERNAL_SERVER_ERROR)?;

        let result = sqlx::query(
    "INSERT INTO users (username, password_hash) VALUES ($1, $2)"
)
.bind(&req.username)
.bind(&password_hash)
.execute(&state.db)
.await;
        match result {
            Ok(_) => Ok(StatusCode::CREATED),
            Err(_) => Err(StatusCode::CONFLICT),// username重複
        }
}

// 実際の通信処理
async fn handle_socket(
    socket: WebSocket,
    state: Arc<AppState>,
    room_id: String,
    username: String,) {
    // この接続が参加するルームの sender を取得
    let room_sender = get_or_create_room_sender(&state, &room_id).await;

    let (mut sender, mut receiver) = socket.split();

    // このルームのメッセージだけを受け取る rx
    let mut rx = room_sender.subscribe();

    // 送信用タスク: ルームの broadcast から受け取って、このクライアントに送信
    let send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            // msg: String -> Utf8Bytes
            let _ = sender.send(Message::Text(msg.into())).await;
            }
    });
    // 受信用ループ: クライアントからのメッセージをこのルームに broadcast
    while let Some(Ok(Message::Text(text))) = receiver.next().await {
        let msg = format!("{}: {}",username,text);
        let _ = room_sender.send(msg);
    }
    send_task.abort();

    
}