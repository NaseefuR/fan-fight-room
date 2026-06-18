const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Client } = require('pg');
const cors = require('cors');
const cron = require('node-cron');
const webpush = require('web-push');
const jwt = require('jsonwebtoken');

// ── Environment config ───────────────────────────────────────────────────────
const PORT = process.env.PORT || 4000;
const DB_URL = process.env.DB_URL || 'postgresql://postgres.cmtlgkouyclnjrkaekws:Naseef@7994529046@aws-1-ap-northeast-2.pooler.supabase.com:6543/postgres?sslmode=require';
const JWT_SECRET = process.env.JWT_SECRET || 'ZmlmYXByZWRpY3RvcnNlY3JldGtleWZvcmp3dDIwMjZzZWN1cmVsb25na2V5dmFsdWU=';
const GROQ_API_KEY = process.env.GROQ_API_KEY || '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';
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || 'BMdUDN9OmbXVBU_Z68gckfAtvKNZd72YJuVXojC4Yp_0-1BoMRL4QH32xTYJEWd3NqUuPOjmQJMNmiI3dfoti6Y';
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || 'Ipp4UYgyLE-_XwAhJtxDLtOMXdEDfzBpQBhXyF9Q2H0';
const VAPID_SUBJECT = 'mailto:support@thefinalthird.com';

webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// ── Database Setup ───────────────────────────────────────────────────────────
// Strip ?sslmode=require from the URL so `pg` doesn't force rejectUnauthorized=true
const cleanDbUrl = DB_URL.replace('?sslmode=require', '').replace('&sslmode=require', '');

const db = new Client({
  connectionString: cleanDbUrl,
  ssl: {
    rejectUnauthorized: false
  }
});
db.connect()
  .then(() => console.log('✅ Connected to PostgreSQL (Supabase)'))
  .catch(e => console.error('❌ DB Connection Error:', e));

// Ensure fan_fight_message table exists
db.query(`
  CREATE TABLE IF NOT EXISTS fan_fight_message (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    mentions JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
`).then(() => console.log('✅ fan_fight_message table ready'));

// ── Express & Socket.io Setup ────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

// ── Health Check (for self-ping) ─────────────────────────────────────────────
app.get('/health', (req, res) => res.json({ status: 'ok', time: new Date() }));

// ── Get Chat History ─────────────────────────────────────────────────────────
app.get('/messages', async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM fan_fight_message ORDER BY created_at ASC LIMIT 500');
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'Failed to fetch messages' });
  }
});

// ── Push Notification Logic ──────────────────────────────────────────────────
async function broadcastPushNotification(authorName, content, mentionsArray) {
  try {
    // Truncate content for notification
    const preview = content.length > 80 ? content.substring(0, 77) + "..." : content;
    
    // Fetch all users who have fanFightPushEnabled = true OR are explicitly mentioned
    // And exclude the author
    const usersQuery = `
      SELECT id, username, fan_fight_push_enabled 
      FROM app_user 
      WHERE username != $1 AND role = 'USER' AND is_approved_by_admin = true
    `;
    const { rows: users } = await db.query(usersQuery, [authorName]);
    
    const userIdsToNotify = [];
    for (const user of users) {
      const isMentioned = mentionsArray.includes(user.username);
      // If mentioned, always notify. If not mentioned, notify if push is enabled.
      if (isMentioned || user.fan_fight_push_enabled) {
        userIdsToNotify.push(user.id);
      }
    }

    if (userIdsToNotify.length === 0) return;

    // Get push subscriptions for these users
    const subsQuery = `
      SELECT endpoint, p256dh, auth, user_id 
      FROM push_subscription 
      WHERE user_id = ANY($1::bigint[])
    `;
    const { rows: subscriptions } = await db.query(subsQuery, [userIdsToNotify]);

    // Send the pushes
    for (const sub of subscriptions) {
      // Find the user to check if they were specifically mentioned
      const user = users.find(u => u.id === sub.user_id);
      const isMentioned = mentionsArray.includes(user.username);
      
      const payload = JSON.stringify({
        title: isMentioned ? `💬 @${authorName} mentioned you in Fan Fight!` : `⚔️ Fan Fight: ${authorName}`,
        body: preview,
        url: '/fan-fight'
      });

      const pushSub = {
        endpoint: sub.endpoint,
        keys: { p256dh: sub.p256dh, auth: sub.auth }
      };

      try {
        await webpush.sendNotification(pushSub, payload);
      } catch (err) {
        if (err.statusCode === 410 || err.statusCode === 404) {
          // Clean up invalid subscription
          await db.query('DELETE FROM push_subscription WHERE endpoint = $1', [sub.endpoint]);
        }
      }
    }
  } catch (e) {
    console.error('Push Notification Error:', e);
  }
}

// ── Socket.io Connection & Auth ──────────────────────────────────────────────
io.use((socket, next) => {
  const token = socket.handshake.auth.token;
  if (!token) return next(new Error('Authentication error'));
  try {
    // Decode the Base64 secret to match Spring Boot's byte array signing
    const decoded = jwt.verify(token, Buffer.from(JWT_SECRET, 'base64'));
    socket.username = decoded.sub; // subject is the username
    next();
  } catch (err) {
    next(new Error('Authentication error'));
  }
});

io.on('connection', (socket) => {
  console.log(`🟢 User connected: ${socket.username}`);

  socket.on('sendMessage', async (data) => {
    try {
      const { content, mentions } = data;
      if (!content || !content.trim()) return;

      // Save to DB
      const query = `
        INSERT INTO fan_fight_message (username, content, mentions, created_at)
        VALUES ($1, $2, $3, NOW())
        RETURNING *
      `;
      const { rows } = await db.query(query, [socket.username, content, JSON.stringify(mentions || [])]);
      const savedMsg = rows[0];

      // Broadcast to all clients
      io.emit('newMessage', savedMsg);

      // Async push notifications
      broadcastPushNotification(socket.username, content, mentions || []);
    } catch (e) {
      console.error('Error saving message:', e);
    }
  });

  socket.on('disconnect', () => {
    console.log(`🔴 User disconnected: ${socket.username}`);
  });
});

// ── Cron: Auto-clear at 10:30 AM IST ─────────────────────────────────────────
cron.schedule('30 10 * * *', async () => {
  console.log('🧹 Running 10:30 AM IST cleanup...');
  try {
    await db.query('TRUNCATE TABLE fan_fight_message RESTART IDENTITY');
    // Notify all clients to clear chat
    io.emit('chatCleared');
    console.log('✅ Fan fight room cleared successfully.');
  } catch (e) {
    console.error('Failed to clear chat:', e);
  }
}, {
  timezone: 'Asia/Kolkata'
});

// ── AI Match Insights ────────────────────────────────────────────────────────
app.get('/insight', async (req, res) => {
  try {
    const { teamA, teamB } = req.query;
    if (!teamA || !teamB) return res.status(400).json({ error: 'Missing teams' });

    const prompt = `You are a football expert. Provide a concise 2-sentence summary of current team forms, key injuries, and a calculated prediction probability for ${teamA} vs ${teamB}. Do not use more than 2 sentences.`;
    
    const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${GROQ_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        messages: [{ role: 'user', content: prompt }]
      })
    });
    
    if (!response.ok) {
      throw new Error(`Groq API error: ${response.statusText}`);
    }
    
    const data = await response.json();
    const insight = data.choices?.[0]?.message?.content || 'Unable to generate insight.';
    
    res.json({ insight });
  } catch (e) {
    console.error('AI Insight Error:', e);
    res.status(500).json({ error: 'Failed to fetch AI insight' });
  }
});

// ── Cron: 10-minute Self Ping ────────────────────────────────────────────────
// If deployed to Render, set RENDER_EXTERNAL_URL in env to ping itself
const RENDER_URL = process.env.RENDER_EXTERNAL_URL;
if (RENDER_URL) {
  cron.schedule('*/10 * * * *', async () => {
    try {
      await fetch(`${RENDER_URL}/health`);
      console.log(`🏓 Self-ping OK to ${RENDER_URL}/health`);
    } catch (e) {
      console.log(`Self-ping failed:`, e.message);
    }
  });
}

// ── Start Server ─────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`🚀 Chat Backend running on port ${PORT}`);
});
