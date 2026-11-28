"""WebSocket（Socket.IO）推送模块。

基于 python-socketio 的 ASGI 集成：
- ws/server.py  : AsyncServer 实例与挂载用的 ASGIApp
- ws/auth.py    : 握手认证（复用 JWT、黑名单、实时会话校验）
- ws/manager.py : 服务端推送封装（按用户 / 全员）
- ws/events.py  : connect / disconnect / 业务事件注册

认证说明：AuthMiddleware 是 BaseHTTPMiddleware，只拦截 HTTP 请求，
WebSocket 握手（含 Socket.IO 的 HTTP 轮询）必须在此模块内独立认证。
"""
