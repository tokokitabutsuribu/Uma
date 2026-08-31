// QR受付・スコア自動精算システム サーバー
// 同一LAN内で動作する簡易サーバーです。
//
// 【全体の流れ】
// 1. 運営が管理画面で「ラウンド開始」→ 5つの選択肢を持つラウンドが受付中になる
// 2. 参加者が読み取り端末でIDをスキャン → 受付中のラウンドがあれば、その場で
//    賭け金（スコアの一部）と選択（単勝1つ or 3連単の順列3つ）を入力して送信
//    → 送信と同時にスコアから賭け金が差し引かれる（＝参加登録）
// 3. 運営が「受付終了」→ 単勝・3連単それぞれの倍率（レート）を自動計算
//    レート = そのラウンドの賭け金合計 ÷ その選択肢に賭けられた金額の合計
// 4. 運営が実際の結果（1〜3着）を入力して「精算」→ 的中者にレートに応じた
//    配当を自動で加算し、スコア変更履歴に記録。ラウンドは終了（settled）となり、
//    次のラウンドが始まるとその参加記録は完全に過去のものになる
//    （＝参加判定は精算完了と同時に消える）

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const os = require('os');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const STORE_PATH = path.join(DATA_DIR, 'store.json');
const INITIAL_SCORE = 1000;
const CHOICE_COUNT = 5;

app.use(express.static('public'));

// ---------- データ永続化 ----------

function loadStore() {
  if (fs.existsSync(STORE_PATH)) {
    try {
      return JSON.parse(fs.readFileSync(STORE_PATH, 'utf-8'));
    } catch (err) {
      console.error('store.json の読み込みに失敗しました。新規作成します。', err);
    }
  }
  return {
    participants: {},
    checkins: [],
    adjustments: [],
    rounds: [],
    nextCheckinSeq: 1,
    nextAdjustmentSeq: 1,
    nextRoundId: 1,
  };
}

let store = loadStore();

function saveStore() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const tmpPath = STORE_PATH + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(store, null, 2), 'utf-8');
  fs.renameSync(tmpPath, STORE_PATH);
}

function nowDisplay() {
  return new Date().toLocaleString('ja-JP');
}

function getOrCreateParticipant(id) {
  let p = store.participants[id];
  let isNew = false;
  if (!p) {
    const now = new Date().toISOString();
    p = {
      id, score: INITIAL_SCORE, checkinCount: 0,
      firstSeen: now, firstSeenDisplay: nowDisplay(),
      lastSeen: now, lastSeenDisplay: nowDisplay(),
    };
    store.participants[id] = p;
    isNew = true;
  }
  return { participant: p, isNew };
}

function participantsAsArray() {
  return Object.values(store.participants);
}

function addAdjustment(participantId, oldScore, newScore, adjustedBy) {
  const adjustment = {
    seq: store.nextAdjustmentSeq++,
    participantId, oldScore, newScore, adjustedBy,
    timestamp: new Date().toISOString(),
    display: nowDisplay(),
  };
  store.adjustments.push(adjustment);
  return adjustment;
}

// ---------- ラウンド関連ヘルパー ----------

// 現在「進行中」（受付中 or 受付終了・結果待ち）のラウンドを1件返す。無ければnull。
function getCurrentRound() {
  if (!store.rounds.length) return null;
  const r = store.rounds[store.rounds.length - 1];
  return r.status === 'settled' ? null : r;
}

io.on('connection', (socket) => {
  console.log('端末が接続しました:', socket.id);

  socket.emit('full-state', {
    participants: participantsAsArray(),
    checkins: store.checkins,
    adjustments: store.adjustments,
    rounds: store.rounds,
  });

  // ---------- QRスキャン（ID読み取り） ----------
  socket.on('qr-scanned', (payload) => {
    const participantId = String(payload.qrText || '').trim();
    if (!participantId) return;
    const deviceName = payload.deviceName || '不明な端末';

    const { participant, isNew } = getOrCreateParticipant(participantId);
    const now = new Date().toISOString();
    participant.checkinCount += 1;
    participant.lastSeen = now;
    participant.lastSeenDisplay = nowDisplay();

    const checkin = {
      seq: store.nextCheckinSeq++,
      participantId, deviceName, timestamp: now, display: nowDisplay(),
    };
    store.checkins.push(checkin);
    saveStore();

    io.emit('new-checkin', { checkin, participant: { ...participant } });
    socket.emit('scan-ack', {
      participantId, score: participant.score, checkinCount: participant.checkinCount, isNew,
    });

    // 受付中のラウンドがあれば、賭け入力用の情報を追加で返す
    const round = getCurrentRound();
    if (round && round.status === 'open') {
      if (round.bets[participantId]) {
        socket.emit('round-bet-exists', { round, bet: round.bets[participantId] });
      } else {
        socket.emit('round-entry', { round, participant: { ...participant } });
      }
    } else if (round && round.status === 'closed') {
      socket.emit('round-closed-notice', { round });
    }

    console.log(`スキャン: ID=${participantId} device=${deviceName} score=${participant.score}`);
  });

  // ---------- 手動スコア編集（管理画面） ----------
  socket.on('update-score', (payload) => {
    const participantId = String(payload.participantId || '').trim();
    const newScore = Number(payload.newScore);
    if (!participantId || !Number.isFinite(newScore)) return;
    const { participant } = getOrCreateParticipant(participantId);
    const oldScore = participant.score;
    participant.score = newScore;
    const adjustment = addAdjustment(participantId, oldScore, newScore, payload.adjustedBy || '不明な操作者');
    saveStore();
    io.emit('score-updated', { participant: { ...participant }, adjustment });
  });

  // ---------- ラウンド開始 ----------
  socket.on('start-round', (payload) => {
    if (getCurrentRound()) {
      socket.emit('round-error', { message: '既に進行中のラウンドがあります。先に精算してください。' });
      return;
    }
    const raw = Array.isArray(payload && payload.choices) ? payload.choices : [];
    const choices = Array.from({ length: CHOICE_COUNT }, (_, i) => {
      const v = String(raw[i] || '').trim();
      return v || String(i + 1);
    });
    const round = {
      id: store.nextRoundId++,
      status: 'open',
      choices,
      createdAt: new Date().toISOString(), createdDisplay: nowDisplay(),
      closedAt: null, closedDisplay: null,
      settledAt: null, settledDisplay: null,
      bets: {},
      pools: null,
      rates: null,
      result: null,
    };
    store.rounds.push(round);
    saveStore();
    io.emit('round-started', { round });
    console.log(`ラウンド${round.id}を開始しました。選択肢: ${choices.join(', ')}`);
  });

  // ---------- 賭け（参加者本人が読み取り端末で入力） ----------
  socket.on('place-bet', (payload) => {
    const round = getCurrentRound();
    if (!round || round.status !== 'open') {
      socket.emit('bet-error', { message: '現在受付中のラウンドはありません。' });
      return;
    }
    const participantId = String(payload.participantId || '').trim();
    const participant = store.participants[participantId];
    if (!participant) {
      socket.emit('bet-error', { message: 'IDが見つかりません。先にQRを読み取ってください。' });
      return;
    }
    if (round.bets[participantId]) {
      socket.emit('bet-error', { message: 'このラウンドではすでに投票済みです。' });
      return;
    }
    const stake = Number(payload.stake);
    if (!Number.isInteger(stake) || stake <= 0) {
      socket.emit('bet-error', { message: '賭け金は1以上の整数で入力してください。' });
      return;
    }
    if (stake > participant.score) {
      socket.emit('bet-error', { message: `スコアが足りません（現在スコア: ${participant.score}）。` });
      return;
    }

    const type = payload.type;
    let selection;
    if (type === 'single') {
      selection = Number(payload.selection);
      if (!Number.isInteger(selection) || selection < 0 || selection >= round.choices.length) {
        socket.emit('bet-error', { message: '選択肢が不正です。' });
        return;
      }
    } else if (type === 'trifecta') {
      const arr = Array.isArray(payload.selection) ? payload.selection.map(Number) : null;
      if (!arr || arr.length !== 3 ||
          arr.some(v => !Number.isInteger(v) || v < 0 || v >= round.choices.length) ||
          new Set(arr).size !== 3) {
        socket.emit('bet-error', { message: '3連単の選択（3つの異なる選択肢の順列）が不正です。' });
        return;
      }
      selection = arr;
    } else {
      socket.emit('bet-error', { message: '賭け方の指定が不正です。' });
      return;
    }

    const oldScore = participant.score;
    participant.score -= stake;
    participant.lastSeen = new Date().toISOString();
    participant.lastSeenDisplay = nowDisplay();

    const bet = {
      participantId, type, selection, stake,
      placedAt: new Date().toISOString(), placedDisplay: nowDisplay(),
      won: null, payout: null,
    };
    round.bets[participantId] = bet;
    const adjustment = addAdjustment(participantId, oldScore, participant.score, `ラウンド${round.id} 賭け金`);
    saveStore();

    socket.emit('bet-placed', { bet, participant: { ...participant } });
    io.emit('round-updated', { round });
    io.emit('score-updated', { participant: { ...participant }, adjustment });
  });

  // ---------- 受付終了（オッズ＝レート確定） ----------
  socket.on('close-round', () => {
    const round = getCurrentRound();
    if (!round || round.status !== 'open') {
      socket.emit('round-error', { message: '受付中のラウンドがありません。' });
      return;
    }
    const singleStakes = {};
    const trifectaStakes = {};
    let singlePool = 0, trifectaPool = 0;

    Object.values(round.bets).forEach(bet => {
      if (bet.type === 'single') {
        singlePool += bet.stake;
        singleStakes[bet.selection] = (singleStakes[bet.selection] || 0) + bet.stake;
      } else {
        trifectaPool += bet.stake;
        const key = bet.selection.join('-');
        trifectaStakes[key] = (trifectaStakes[key] || 0) + bet.stake;
      }
    });

    const singleRates = {};
    round.choices.forEach((_, idx) => {
      singleRates[idx] = singleStakes[idx] ? +(singlePool / singleStakes[idx]).toFixed(2) : null;
    });
    const trifectaRates = {};
    Object.keys(trifectaStakes).forEach(key => {
      trifectaRates[key] = +(trifectaPool / trifectaStakes[key]).toFixed(2);
    });

    round.status = 'closed';
    round.closedAt = new Date().toISOString();
    round.closedDisplay = nowDisplay();
    round.pools = { single: singlePool, trifecta: trifectaPool };
    round.rates = { single: singleRates, trifecta: trifectaRates };
    saveStore();
    io.emit('round-closed', { round });
    console.log(`ラウンド${round.id}の受付を終了し、レートを確定しました。`);
  });

  // ---------- 結果入力・自動精算 ----------
  socket.on('settle-round', (payload) => {
    const round = getCurrentRound();
    if (!round || round.status !== 'closed') {
      socket.emit('round-error', { message: '受付終了状態のラウンドがありません。先に「受付終了」を行ってください。' });
      return;
    }
    const result = payload && payload.result ? payload.result : {};
    const first = Number(result.first), second = Number(result.second), third = Number(result.third);
    const idxOk = v => Number.isInteger(v) && v >= 0 && v < round.choices.length;
    if (![first, second, third].every(idxOk) || new Set([first, second, third]).size !== 3) {
      socket.emit('round-error', { message: '1〜3着として、異なる選択肢を3つ選んでください。' });
      return;
    }

    const trifectaKey = [first, second, third].join('-');
    const updatedParticipants = [];
    const newAdjustments = [];

    Object.values(round.bets).forEach(bet => {
      const participant = store.participants[bet.participantId];
      if (!participant) return;
      let won, rate;
      if (bet.type === 'single') {
        won = bet.selection === first;
        rate = round.rates.single[bet.selection] || 0;
      } else {
        won = bet.selection.join('-') === trifectaKey;
        rate = round.rates.trifecta[bet.selection.join('-')] || 0;
      }
      let payout = 0;
      if (won && rate > 0) {
        payout = Math.round(bet.stake * rate);
        const oldScore = participant.score;
        participant.score += payout;
        participant.lastSeen = new Date().toISOString();
        participant.lastSeenDisplay = nowDisplay();
        const adjustment = addAdjustment(bet.participantId, oldScore, participant.score, `ラウンド${round.id} 配当`);
        newAdjustments.push(adjustment);
        updatedParticipants.push({ ...participant });
      }
      bet.won = won;
      bet.payout = payout;
    });

    round.status = 'settled';
    round.result = { first, second, third };
    round.settledAt = new Date().toISOString();
    round.settledDisplay = nowDisplay();
    saveStore();

    io.emit('round-settled', { round, updatedParticipants });
    newAdjustments.forEach((adjustment, i) => {
      io.emit('score-updated', { participant: updatedParticipants[i], adjustment });
    });
    console.log(`ラウンド${round.id}を精算しました。結果: ${round.choices[first]} / ${round.choices[second]} / ${round.choices[third]}`);
  });

  // ---------- 全データ初期化（本番前テスト用） ----------
  socket.on('reset-all', () => {
    store = {
      participants: {}, checkins: [], adjustments: [], rounds: [],
      nextCheckinSeq: 1, nextAdjustmentSeq: 1, nextRoundId: 1,
    };
    saveStore();
    io.emit('state-cleared');
    console.log('全データをリセットしました。');
  });

  socket.on('disconnect', () => {
    console.log('端末が切断しました:', socket.id);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  const nets = os.networkInterfaces();
  const addresses = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) addresses.push(net.address);
    }
  }
  console.log('==========================================');
  console.log(`サーバー起動: ポート ${PORT}`);
  console.log(`データ保存先: ${STORE_PATH}`);
  console.log('同じWi-Fi内の他の端末から、以下のURLでアクセスしてください:');
  addresses.forEach((addr) => {
    console.log(`  読み取り端末用: http://${addr}:${PORT}/scan.html`);
    console.log(`  管理デバイス用: http://${addr}:${PORT}/admin.html`);
  });
  console.log('==========================================');
});
