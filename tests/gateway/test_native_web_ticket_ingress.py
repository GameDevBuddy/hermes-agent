"""The App's web-ticket ingress must reach the original native connection capture.

Use the existing lower Job/stdin fixture, but real ticket mint/authentication,
GatewayRuntimeAPI routing and canonical WebSocket handling. No provider or editor.
"""
import asyncio
import json

from starlette.requests import Request
from starlette.websockets import WebSocket

from tests.gateway.native_app_context_fixture import Case, NONCE


def test_app_web_ticket_can_stage_selected_tools(monkeypatch, tmp_path):
    from gateway.runtime_bootstrap import TicketStore
    from hermes_cli import web_server as web
    from hermes_cli.web_routers.chat_ws import gateway_ws
    from hermes_cli.web_routers.webapp import issue_socket_ticket
    from tui_gateway import ws as socket_handler

    case = Case(monkeypatch, tmp_path)
    token = case.web._SESSION_TOKEN
    monkeypatch.setattr(web, '_SESSION_TOKEN', token)
    monkeypatch.setattr(web, '_DASHBOARD_EMBEDDED_CHAT_ENABLED', True)
    # Unrelated first-chat background work is outside this transport regression.
    monkeypatch.setattr('hermes_cli.mcp_startup.start_deferred_mcp_discovery_now', lambda: None)
    monkeypatch.setattr(socket_handler, '_start_backend_liveness', lambda: None)
    state = case.web.app.state
    for target in (state, web.app.state):
        for name, value in {'ui_surface': 'webapp', 'auth_required': False,
                            'bound_host': '127.0.0.1'}.items():
            monkeypatch.setattr(target, name, value, raising=False)
    case.runner.session_ticket_store = TicketStore(case.authority.instance_id, [case.authority.profile_id])

    async def route(scope, receive, send):
        await gateway_ws(WebSocket(scope, receive, send))

    case.api.app = route

    async def exercise():
        scope = {'type': 'http', 'method': 'POST', 'path': '/api/webapp/socket-ticket',
                 'scheme': 'http', 'query_string': b'socket=gateway',
                 'client': ('127.0.0.1', 1234), 'server': ('127.0.0.1', 9999),
                 'headers': [(b'host', b'127.0.0.1:9999'),
                             (b'authorization', ('Bearer ' + token).encode())], 'app': case.web.app}
        response = await issue_socket_ticket(Request(scope), socket='gateway')
        ticket = json.loads(response.body)['ticket']
        protocols = ['hermes-gateway-v1', 'hermes-gateway-ticket.' + ticket]
        incoming, outgoing = asyncio.Queue(), asyncio.Queue()
        await incoming.put({'type': 'websocket.connect'})
        scope = {'type': 'websocket', 'path': '/api/ws', 'scheme': 'ws', 'query_string': b'',
                 'client': ('127.0.0.1', 1234), 'server': ('127.0.0.1', 9999),
                 'headers': [(b'host', b'127.0.0.1:9999'),
                             (b'sec-websocket-protocol', ', '.join(protocols).encode())],
                 'subprotocols': protocols}
        previous = set(case.runtime.connections)
        task = asyncio.create_task(case.api(scope, incoming.get, outgoing.put))
        try:
            while True:
                message = await asyncio.wait_for(outgoing.get(), 10)
                assert message['type'] != 'websocket.close', message
                if message['type'] == 'websocket.send':
                    frame = json.loads(message['text'])
                    if frame.get('params', {}).get('type') == 'gateway.ready':
                        break
            captured = [value for key, value in case.runtime.connections.items() if key not in previous]
            assert len(captured) == 1, 'authenticated App web-ticket socket lost its original connection'
            connection = captured[0].connection
            assert connection._operator and 'session:operator' in connection.actor.capabilities
            # The lower session fixture is already loaded; subscribe this actual authenticated peer.
            case.live.subscribers['web-ticket'] = connection.actor
            await incoming.put({'type': 'websocket.receive', 'text': json.dumps({
                'jsonrpc': '2.0', 'id': 1, 'method': 'native.context.stage', 'params': case.params()})})
            while True:
                message = await asyncio.wait_for(outgoing.get(), 10)
                if message['type'] == 'websocket.send':
                    frame = json.loads(message['text'])
                    if frame.get('id') == 1:
                        break
            assert frame == {'jsonrpc': '2.0', 'id': 1, 'result': {
                'staged': True, 'context_id': 'context-fixture', 'nonce': NONCE}}
            assert not connection._gdb_original_app_contexts
            assert not case.source._app_context_requests  # Still requires the original private stdin bind.
        finally:
            await incoming.put({'type': 'websocket.disconnect', 'code': 1000})
            await asyncio.wait_for(task, 10)

    try:
        case.run(exercise())
    finally:
        case.close()
