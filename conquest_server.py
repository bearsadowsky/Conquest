"""Conquest multiplayer server (WebSocket version, works on Replit).

Run:  python conquest_server.py
Env:  PORT (default 8080)

The server is authoritative for player HP and the 8 target dummies.
Clients send their position ~20x/sec and report hits; the server applies
the damage and broadcasts the results.
"""
import asyncio
import json
import math
import os
import random
from http import HTTPStatus

MAX_PLAYERS = 5
TARGET_COUNT = 8
MAX_HP = 100.0
TARGET_REVIVE_DELAY = 3.0
SNAP_INTERVAL = 0.05          # 20 snapshots per second
MAX_DAMAGE = 500.0            # sanity clamp on a single reported hit


class Player:
    def __init__(self, pid, name, conn):
        self.id = pid
        self.name = name
        self.conn = conn
        self.hp = MAX_HP
        # x, y, z, ry, weapon, punch_t, pitch  (hp is appended when sending)
        self.state = [(pid - 3) * 2.0, 0.0, 0.0, 0.0, 0, -1, 0.0]


class Game:
    def __init__(self):
        self.seed = random.randrange(1, 2**31)
        self.players = {}
        self.target_pos = []
        for i in range(TARGET_COUNT):
            ang = i / TARGET_COUNT * math.tau + random.uniform(-0.2, 0.2)
            r = random.uniform(18, 45)
            self.target_pos.append([round(math.cos(ang) * r, 2), round(math.sin(ang) * r, 2)])
        self.target_hp = [100.0] * TARGET_COUNT

    # ---------------------------------------------------------- sending
    async def _send(self, player, text):
        try:
            await player.conn.send(text)
        except Exception:
            pass    # that connection is closing; its handler will clean up

    async def broadcast(self, obj, exclude=None):
        text = json.dumps(obj, separators=(',', ':'))
        targets = [p for p in list(self.players.values()) if p is not exclude]
        if targets:
            await asyncio.gather(*(self._send(p, text) for p in targets))

    # ---------------------------------------------------------- joining
    def free_id(self):
        for pid in range(1, MAX_PLAYERS + 1):
            if pid not in self.players:
                return pid
        return None

    async def add_player(self, conn, name):
        """Returns the Player, or None if the server is full."""
        pid = self.free_id()
        if pid is None:
            await conn.send(json.dumps({'t': 'full'}))
            return None
        name = (str(name).strip() or f'Player {pid}')[:16]
        player = Player(pid, name, conn)
        welcome = {
            't': 'welcome',
            'id': pid,
            'seed': self.seed,
            'players': [{'id': p.id, 'name': p.name} for p in self.players.values()],
            'thp': self.target_hp,
            'targets': self.target_pos,
        }
        await conn.send(json.dumps(welcome, separators=(',', ':')))
        self.players[pid] = player
        await self.broadcast({'t': 'join', 'id': pid, 'name': name}, exclude=player)
        print(f'+ {name} joined as player {pid} ({len(self.players)} online)')
        return player

    async def remove_player(self, player):
        if self.players.get(player.id) is player:
            del self.players[player.id]
            await self.broadcast({'t': 'leave', 'id': player.id})
            print(f'- {player.name} left ({len(self.players)} online)')

    # ---------------------------------------------------------- messages
    async def handle(self, player, m):
        t = m.get('t')
        if t == 's':
            try:
                player.state = [float(m['x']), float(m['y']), float(m['z']), float(m['ry']),
                                int(m['w']), float(m['pt']), float(m['pp'])]
            except (KeyError, TypeError, ValueError):
                pass
        elif t == 'fire':
            await self.broadcast({'t': 'fire', 'b': m.get('b'), 'p': m.get('p'), 'v': m.get('v')},
                                 exclude=player)
        elif t == 'explode':
            await self.broadcast({'t': 'explode', 'b': m.get('b'), 'p': m.get('p')}, exclude=player)
            await self.apply_hits(player, m.get('h'))
        elif t == 'punch':
            await self.apply_hits(player, m.get('h'))

    async def apply_hits(self, attacker, hits):
        if not isinstance(hits, list):
            return
        for h in hits:
            try:
                kind = h[0]
                if kind == 't':
                    await self.hit_target(int(h[1]), float(h[2]), float(h[3]), float(h[4]))
                elif kind == 'p':
                    await self.hit_player(attacker, int(h[1]), float(h[2]))
            except (IndexError, TypeError, ValueError):
                continue

    async def hit_target(self, i, amount, kx, kz):
        if not 0 <= i < TARGET_COUNT or self.target_hp[i] <= 0:
            return
        amount = max(0.0, min(amount, MAX_DAMAGE))
        self.target_hp[i] = max(0.0, self.target_hp[i] - amount)
        await self.broadcast({'t': 'thit', 'i': i, 'hp': round(self.target_hp[i], 1),
                              'kx': kx, 'kz': kz})
        if self.target_hp[i] <= 0:
            asyncio.get_running_loop().call_later(
                TARGET_REVIVE_DELAY, lambda: asyncio.ensure_future(self.revive_target(i)))

    async def revive_target(self, i):
        self.target_hp[i] = 100.0
        await self.broadcast({'t': 'trevive', 'i': i})

    async def hit_player(self, attacker, victim_id, amount):
        victim = self.players.get(victim_id)
        if victim is None:
            return
        amount = max(0.0, min(amount, MAX_DAMAGE))
        victim.hp -= amount
        if victim.hp <= 0:
            victim.hp = MAX_HP
            if victim is attacker:
                text = f'{victim.name} blew themselves up!'
            else:
                text = f'{attacker.name} flattened {victim.name}!'
            await self.broadcast({'t': 'died', 'id': victim.id, 'text': text})

    # ---------------------------------------------------------- snapshots
    async def snapshot_loop(self):
        while True:
            await asyncio.sleep(SNAP_INTERVAL)
            if self.players:
                snap = {str(p.id): [round(v, 2) if isinstance(v, float) else v for v in p.state]
                        + [round(p.hp, 1)] for p in self.players.values()}
                await self.broadcast({'t': 'snap', 'p': snap})


game = Game()


# ---------------------------------------------------------------- websocket glue
async def handler(ws):
    player = None
    try:
        first = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        if first.get('t') != 'hello':
            return
        player = await game.add_player(ws, first.get('name', ''))
        if player is None:
            return
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if isinstance(msg, dict):
                await game.handle(player, msg)
    except Exception:
        pass
    finally:
        if player is not None:
            await game.remove_player(player)


def health_check(connection, request):
    """Replit pings plain HTTP on '/' to see if the app is alive; answer 200."""
    if request.headers.get('Upgrade', '').lower() != 'websocket':
        return connection.respond(HTTPStatus.OK, 'Conquest server is running.\n')
    return None


async def main():
    from websockets.asyncio.server import serve
    port = int(os.environ.get('PORT', 8080))
    async with serve(handler, '0.0.0.0', port, process_request=health_check):
        print(f'Conquest server listening on port {port}')
        await game.snapshot_loop()


if __name__ == '__main__':
    asyncio.run(main())
