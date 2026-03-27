require("dotenv").config();
const express = require("express");
const cors = require("cors");
const admin = require("firebase-admin");

const app = express();
const PORT = process.env.PORT || 4000;

// ---------------------------------------------------------------------------
// Firebase Initialization
// ---------------------------------------------------------------------------
const serviceAccountKey = process.env.FIREBASE_SERVICE_ACCOUNT 
  ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT) 
  : require("./firebase-service-account.json");

admin.initializeApp({ credential: admin.credential.cert(serviceAccountKey) });
const db = admin.firestore();

app.use(cors());
app.use(express.json());

// ---------------------------------------------------------------------------
// Category helpers
// ---------------------------------------------------------------------------
const CATEGORY_META = {
  Food: { icon: "🍔", color: "#F59E0B" },
  Transport: { icon: "🚗", color: "#3B82F6" },
  Fuel: { icon: "⛽", color: "#F97316" },
  Shopping: { icon: "🛍️", color: "#8B5CF6" },
  Bills: { icon: "⚡", color: "#10B981" },
  Entertainment: { icon: "🎬", color: "#EC4899" },
  Travel: { icon: "✈️", color: "#06B6D4" },
  "Money Received": { icon: "💸", color: "#16A34A" },
  "Person Transfer": { icon: "👤", color: "#6366F1" },
  Others: { icon: "💰", color: "#64748B" },
};

function getCategoryIcon(cat) { return (CATEGORY_META[cat] || CATEGORY_META.Others).icon; }
function getCategoryColor(cat) { return (CATEGORY_META[cat] || CATEGORY_META.Others).color; }

// ---------------------------------------------------------------------------
// Enhanced Parser
// ---------------------------------------------------------------------------
function normalizeAmount(text = "") {
  const patterns = [
    /₹\s*([\d,]+(?:\.\d{1,2})?)/,
    /Rs\.?\s*([\d,]+(?:\.\d{1,2})?)/i,
    /INR\.?\s*([\d,]+(?:\.\d{1,2})?)/i,
    /Rupees?\s*([\d,]+(?:\.\d{1,2})?)/i,
    /(?:paid|received|sent|debited|credited|collected|of)\s+(?:Rs\.?|₹|INR)?\s*([\d,]+(?:\.\d{1,2})?)/i,
  ];
  for (const regex of patterns) {
    const m = text.match(regex);
    if (m && m[1]) {
      const num = Number(m[1].replace(/,/g, ""));
      if (num > 0) return num;
    }
  }
  return 0;
}

const NAME_STOP = /\b(via|using|on|for|rs\.?|inr|upi|you|your|has|was|is|paid|received|sent|debited|credited|collected|payment|amount|transaction|bank|a\/c|ac|account|ref|txn|id|no|number|at|with|through|by|the|successful|success|completed|done|rupees?)\b/i;

function extractName(text = "") {
  const patterns = [
    /\bpaid\s+to\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\breceived\s+from\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bcredited\s+(?:by|from)\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bdebited\s+(?:to|for)\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bsent\s+to\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bcollected\s+from\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bto\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bfrom\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bat\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bbeneficiary\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bmerchant\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
  ];

  for (const regex of patterns) {
    const m = text.match(regex);
    if (m && m[1]) {
      let name = m[1].trim();
      const stop = name.match(NAME_STOP);
      if (stop && stop.index !== undefined && stop.index > 0) {
        name = name.substring(0, stop.index).trim();
      }
      name = name.replace(/[\s.\-,]+$/, "");
      if (name.length > 1) return name;
    }
  }
  return "";
}

function detectDirection(text = "") {
  const l = text.toLowerCase();
  const creditWords = /(received|credited|money received|collected|has sent you|refund|cashback)/;
  const debitWords = /(paid|sent|debited|payment of|spent|charged|deducted|purchase)/;

  if (creditWords.test(l) && !debitWords.test(l)) return "credit";
  if (debitWords.test(l)) return "debit";
  if (/\bfrom\b/.test(l) && !/\bto\b/.test(l)) return "credit";
  return "debit";
}

function detectCategory(name = "", body = "") {
  const t = `${name} ${body}`.toLowerCase();
  if (/(zomato|swiggy|domino|pizza|starbucks|cafe|restaurant|food|mcdonald|subway|burger|kitchen|biryani|chai|tea|bakery|dhaba|mess)/.test(t)) return "Food";
  if (/(uber|ola|rapido|metro|cab|ride|transport|auto|rickshaw|taxi)/.test(t)) return "Transport";
  if (/(petrol|fuel|indianoil|bharat petroleum|bpcl|hpcl|pump|diesel|cng)/.test(t)) return "Fuel";
  if (/(amazon|flipkart|myntra|shopping|store|decathlon|ajio|meesho|nykaa|croma|reliance digital)/.test(t)) return "Shopping";
  if (/(jio|airtel|mseb|electric|bill|recharge|postpaid|broadband|wifi|internet|gas|water|electricity|vi|vodafone|bsnl)/.test(t)) return "Bills";
  if (/(pvr|bookmyshow|netflix|spotify|movie|cinema|prime|hotstar|disney|youtube|gaming|game|inox)/.test(t)) return "Entertainment";
  if (/(flight|travel|rail|train|hotel|irctc|makemytrip|goibibo|booking|airbnb|oyo)/.test(t)) return "Travel";
  return "Person Transfer";
}

function applyAlias(name, aliases = {}) {
  const key = name.toLowerCase();
  return aliases[key] || name;
}

function parseNotification(payload, aliases = {}) {
  const title = payload.title || "";
  const text = payload.text || "";
  const appName = payload.appName || "";
  const upiId = payload.upiId || "";

  const combined = `${title} ${text}`.trim();
  const amount = normalizeAmount(combined);
  const direction = detectDirection(combined);

  let rawName =
    payload.payeeName ||
    extractName(title) ||
    extractName(text) ||
    extractName(combined) ||
    title ||
    "Unknown";

  const displayName = applyAlias(rawName, aliases);
  const category = direction === "credit" ? "Money Received" : detectCategory(displayName, combined);

  return { sourceApp: appName, rawTitle: title, rawText: text, upiId, amount, direction, rawName, name: displayName, category };
}

// ---------------------------------------------------------------------------
// Middlewares / Helpers
// ---------------------------------------------------------------------------
app.get("/health", (_req, res) => res.json({ ok: true }));

async function getUserDoc() {
  const d = await db.collection("users").doc("me").get();
  return d.exists ? d.data() : null;
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

app.get("/api/user", async (req, res) => {
  try {
    const user = await getUserDoc();
    res.json({ success: true, data: user });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post("/api/user", async (req, res) => {
  try {
    const { name, upiId, budget } = req.body || {};
    if (!name || !upiId || budget == null) {
      return res.status(400).json({ success: false, error: "name, upiId, and budget required" });
    }

    const current = await getUserDoc();
    const isNew = !current;

    const dataObj = {
      id: isNew ? "u_" + Date.now() : current.id,
      name: name.trim(),
      upiId: upiId.trim(),
      balance: isNew ? 0 : current.balance,
      budget: Number(budget),
      aliases: current ? current.aliases || {} : {},
      contacts: current ? current.contacts || {} : {},
      createdAt: isNew ? new Date().toISOString() : current.createdAt,
      updatedAt: new Date().toISOString(),
    };

    await db.collection("users").doc("me").set(dataObj, { merge: true });
    res.json({ success: true, data: dataObj });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.delete("/api/user/reset", async (req, res) => {
  try {
    const batch = db.batch();
    const collections = ["transactions", "notifications", "ledger"];
    for (const c of collections) {
      const snap = await db.collection(c).get();
      snap.docs.forEach(d => batch.delete(d.ref));
    }
    // Also reset main balance
    batch.update(db.collection("users").doc("me"), { balance: 0 });
    await batch.commit();
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/api/transactions", async (_req, res) => {
  try {
    const s = await db.collection("transactions").orderBy("createdAt", "desc").get();
    res.json({ success: true, data: s.docs.map(d => d.data()) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/api/notifications", async (_req, res) => {
  try {
    const s = await db.collection("notifications").orderBy("createdAt", "desc").get();
    res.json({ success: true, data: s.docs.map(d => d.data()) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/api/summary", async (_req, res) => {
  try {
    const [userDoc, txSnap, lgrSnap] = await Promise.all([
      getUserDoc(),
      db.collection("transactions").get(),
      db.collection("ledger").where("settled", "==", false).get(),
    ]);

    const tx = txSnap.docs.map(d => d.data());
    const spent = tx.filter(t => t.direction === "debit").reduce((s, t) => s + t.amount, 0);
    const received = tx.filter(t => t.direction === "credit").reduce((s, t) => s + t.amount, 0);

    const categoryTotals = {};
    for (const item of tx) {
      if (item.direction !== "debit") continue;
      categoryTotals[item.category] = (categoryTotals[item.category] || 0) + item.amount;
    }
    const topCategory = Object.keys(categoryTotals).length
      ? Object.keys(categoryTotals).reduce((a, b) => (categoryTotals[a] > categoryTotals[b] ? a : b)) : "None";

    const ledger = lgrSnap.docs.map(d => d.data());
    const totalLent = ledger.filter(e => e.type === "lent").reduce((s, e) => s + e.amount, 0);
    const totalBorrowed = ledger.filter(e => e.type === "borrowed").reduce((s, e) => s + e.amount, 0);

    res.json({
      success: true,
      data: {
        totalSpent: spent,
        totalReceived: received,
        balance: userDoc ? userDoc.balance : 0,
        budget: userDoc ? userDoc.budget : 0,
        topCategory, count: tx.length, categoryTotals,
        totalLent, totalBorrowed,
      },
    });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post("/api/ingest-notification", async (req, res) => {
  try {
    const userDoc = await getUserDoc();
    if (!userDoc) {
      return res.status(400).json({ success: false, error: "User onboarding not completed" });
    }

    const parsed = parseNotification(req.body || {}, userDoc.aliases || {});
    if (!parsed.amount || parsed.amount <= 0) {
      return res.status(400).json({ success: false, error: "Could not detect amount from notification" });
    }

    const tx = {
      id: "TX" + Date.now(),
      name: parsed.name, rawName: parsed.rawName, upiId: parsed.upiId || "", amount: parsed.amount,
      category: parsed.category, direction: parsed.direction, sourceApp: parsed.sourceApp,
      icon: getCategoryIcon(parsed.category), color: getCategoryColor(parsed.category),
      rawTitle: parsed.rawTitle, rawText: parsed.rawText, createdAt: new Date().toISOString(),
    };

    let newBalance = userDoc.balance;
    if (parsed.direction === "debit") newBalance -= parsed.amount;
    else newBalance += parsed.amount;

    const nf = {
      id: "NF" + Date.now(),
      title: parsed.direction === "credit" ? `Received ₹${parsed.amount} from ${parsed.name}` : `Paid ₹${parsed.amount} to ${parsed.name}`,
      body: `${parsed.category} • ${parsed.sourceApp}`,
      txId: tx.id, createdAt: new Date().toISOString(),
    };

    const batch = db.batch();
    batch.set(db.collection("users").doc("me"), { balance: newBalance }, { merge: true });
    batch.set(db.collection("transactions").doc(tx.id), tx);
    batch.set(db.collection("notifications").doc(nf.id), nf);
    await batch.commit();

    res.json({ success: true, data: { transaction: tx, notification: nf } });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// ---- Aliases ----
app.get("/api/aliases", async (_req, res) => {
  const d = await getUserDoc();
  res.json({ success: true, data: d ? d.aliases || {} : {} });
});

app.post("/api/alias", async (req, res) => {
  try {
    const { originalName, alias } = req.body || {};
    if (!originalName || !alias) return res.status(400).json({ success: false, error: "originalName and alias required" });
    
    // Set in UserDoc
    const key = originalName.toLowerCase();
    await db.collection("users").doc("me").set({ aliases: { [key]: alias } }, { merge: true });

    // Update existing transactions safely via batch matching
    const txSnap = await db.collection("transactions").get();
    const batch = db.batch();
    txSnap.docs.forEach(d => {
      const tx = d.data();
      if (tx.rawName && tx.rawName.toLowerCase() === key) batch.update(d.ref, { name: alias });
    });
    await batch.commit(); // Could exceed 500 limits for massive users but fine for us
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

app.delete("/api/alias/:name", async (req, res) => {
  try {
    const key = decodeURIComponent(req.params.name).toLowerCase();
    await db.collection("users").doc("me").update({ [`aliases.${key}`]: admin.firestore.FieldValue.delete() });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ---- Contacts ----
app.post("/api/contacts", async (req, res) => {
  try {
    const { contacts } = req.body || {};
    if (!contacts) return res.status(400).json({ success: false, error: "contacts required" });
    await db.collection("users").doc("me").set({ contacts }, { merge: true });
    res.json({ success: true, count: Object.keys(contacts).length });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});
app.get("/api/contacts", async (_req, res) => {
  const d = await getUserDoc(); res.json({ success: true, data: d ? d.contacts || {} : {} });
});

// ---- Ledger ----
app.get("/api/ledger", async (_req, res) => {
  try {
    const s = await db.collection("ledger").orderBy("createdAt", "desc").get();
    res.json({ success: true, data: s.docs.map(d => d.data()) });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

app.post("/api/ledger", async (req, res) => {
  try {
    const { personName, amount, type, note } = req.body || {};
    if (!personName || !amount || !type) return res.status(400).json({ success: false, error: "personName, amount, type required" });

    const entry = {
      id: "LD" + Date.now(), personName: personName.trim(), amount: Number(amount), type,
      note: (note || "").trim(), settled: false, createdAt: new Date().toISOString(), settledAt: null,
    };
    await db.collection("ledger").doc(entry.id).set(entry);
    res.json({ success: true, data: entry });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

app.put("/api/ledger/:id", async (req, res) => {
  try {
    const dRef = db.collection("ledger").doc(req.params.id);
    const d = await dRef.get();
    if (!d.exists) return res.status(404).json({ success: false, error: "Ledger entry not found" });

    const { settled, personName, amount, note } = req.body || {};
    const updates = {};
    if (settled !== undefined) { updates.settled = Boolean(settled); updates.settledAt = updates.settled ? new Date().toISOString() : null; }
    if (personName) updates.personName = personName.trim();
    if (amount) updates.amount = Number(amount);
    if (note !== undefined) updates.note = note.trim();

    await dRef.update(updates);
    const updated = await dRef.get();
    res.json({ success: true, data: updated.data() });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

app.delete("/api/ledger/:id", async (req, res) => {
  try { await db.collection("ledger").doc(req.params.id).delete(); res.json({ success: true }); }
  catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

app.listen(PORT, () => console.log(`FlowLedger backend deployed to FireBase! On Port: ${PORT}`));