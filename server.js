require("dotenv").config();
const express = require("express");
const cors = require("cors");
const admin = require("firebase-admin");

const app = express();
const PORT = process.env.PORT || 4000;

const serviceAccountKey = process.env.FIREBASE_SERVICE_ACCOUNT
  ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
  : require("./firebase-service-account.json");

admin.initializeApp({ credential: admin.credential.cert(serviceAccountKey) });
const db = admin.firestore();

app.use(cors());
app.use(express.json());

const CATEGORY_META = {
  Food: { icon: "🍔", color: "#F59E0B" },
  Groceries: { icon: "🛒", color: "#10B981" },
  Transport: { icon: "🚗", color: "#3B82F6" },
  Fuel: { icon: "⛽", color: "#F97316" },
  Shopping: { icon: "🛍️", color: "#8B5CF6" },
  Bills: { icon: "⚡", color: "#06B6D4" },
  Entertainment: { icon: "🎬", color: "#EC4899" },
  Travel: { icon: "✈️", color: "#6366F1" },
  Health: { icon: "💊", color: "#EF4444" },
  Education: { icon: "📚", color: "#FBBF24" },
  Investments: { icon: "📈", color: "#14B8A6" },
  "Money Received": { icon: "💸", color: "#16A34A" },
  "Person Transfer": { icon: "👤", color: "#8B5CF6" },
  Others: { icon: "💰", color: "#64748B" },
};

const AMOUNT_PATTERNS = [
  /(?:₹|rs\.?|inr|rupees?)\s*([\d,]+(?:\.\d{1,2})?)/i,
  /(?:paid|received|sent|debited|credited|collected|payment of|amount of)\s+(?:₹|rs\.?|inr)?\s*([\d,]+(?:\.\d{1,2})?)/i,
  /\b([\d,]+(?:\.\d{1,2})?)\s*(?:rs|inr)\b/i,
];

const NAME_STOP = /\b(via|using|on|for|rs\.?|inr|upi|you|your|has|was|is|paid|received|sent|debited|credited|collected|payment|amount|transaction|bank|a\/c|ac|account|ref|txn|id|no|number|at|with|through|by|the|successful|success|completed|done|rupees?)\b/i;

const CATEGORY_RULES = [
  { category: "Fuel", keywords: ["petrol", "diesel", "fuel", "indianoil", "bharat petroleum", "bpcl", "hpcl", "ioc", "shell", "nayara", "pump", "filling station", "hindustan petroleum"] },
  { category: "Groceries", keywords: ["blinkit", "zepto", "instamart", "bigbasket", "dmart", "reliance fresh", "grocery", "grocers", "supermarket", "kirana", "mart", "spencer", "more retail"] },
  { category: "Food", keywords: ["zomato", "swiggy", "dominos", "pizza", "starbucks", "restaurant", "cafe", "eatclub", "burger", "biryani", "bakery", "tea", "chai", "subway", "mcdonald", "kfc"] },
  { category: "Bills", keywords: ["jio", "airtel", "vi", "vodafone", "bsnl", "recharge", "postpaid", "prepaid", "broadband", "electricity", "electric", "water bill", "gas bill", "wifi", "internet", "dth", "tata play", "fastag", "bescom", "mseb", "torrent power", "billdesk"] },
  { category: "Transport", keywords: ["uber", "ola", "rapido", "metro", "cab", "taxi", "auto", "rickshaw", "namma yatri", "blusmart", "indrive", "parking", "toll"] },
  { category: "Travel", keywords: ["irctc", "makemytrip", "goibibo", "airbnb", "oyo", "flight", "train", "rail", "hotel", "booking", "redbus", "abhibus", "cleartrip", "ticket"] },
  { category: "Entertainment", keywords: ["netflix", "spotify", "prime video", "amazon prime", "bookmyshow", "hotstar", "disney", "youtube premium", "sony liv", "zee5", "movie", "cinema", "game", "steam"] },
  { category: "Health", keywords: ["apollo", "netmeds", "pharmeasy", "1mg", "medical", "hospital", "clinic", "doctor", "diagnostics", "lab", "medicine"] },
  { category: "Education", keywords: ["school", "college", "university", "course", "udemy", "coursera", "academy", "tuition", "fee"] },
  { category: "Investments", keywords: ["zerodha", "groww", "upstox", "mutual fund", "sip", "insurance", "policy", "lic", "premium", "coin", "kuvera", "indmoney", "stock"] },
  { category: "Shopping", keywords: ["amazon", "flipkart", "myntra", "ajio", "meesho", "nykaa", "croma", "decathlon", "reliance digital", "zara", "hm", "h&m", "shopping", "store"] },
];

function getCategoryIcon(cat) {
  return (CATEGORY_META[cat] || CATEGORY_META.Others).icon;
}

function getCategoryColor(cat) {
  return (CATEGORY_META[cat] || CATEGORY_META.Others).color;
}

function normalizeText(text = "") {
  return text
    .toLowerCase()
    .replace(/[|,:;()[\]{}_*#]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeAmount(text = "") {
  for (const regex of AMOUNT_PATTERNS) {
    const match = text.match(regex);
    if (match && match[1]) {
      const num = Number(match[1].replace(/,/g, ""));
      if (num > 0) return num;
    }
  }
  return 0;
}

function extractName(text = "") {
  const patterns = [
    /\bpaid\s+to\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\breceived\s+from\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bcredited\s+(?:by|from)\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bdebited\s+(?:to|for)\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bsent\s+to\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bcollected\s+from\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bmerchant\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
    /\bbeneficiary\s+([A-Z][A-Za-z0-9&.'\- ]{1,40})/i,
  ];

  for (const regex of patterns) {
    const match = text.match(regex);
    if (match && match[1]) {
      let name = match[1].trim();
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

function cleanDisplayName(name = "") {
  return name
    .replace(/\b(?:upi|txn|transaction|ref|bank|payment|successful|success|debited|credited|received|paid)\b/gi, " ")
    .replace(/[|,:;()[\]{}_*#]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeSourceApp(appName = "") {
  const normalized = appName.toLowerCase();
  if (normalized.includes("com.android.mms") || normalized.includes("messaging") || normalized.includes("messages")) {
    return "SMS Alerts";
  }
  if (normalized.includes("phonepe")) return "PhonePe";
  if (normalized.includes("paytm")) return "Paytm";
  if (normalized.includes("google") || normalized.includes("paisa")) return "GPay";
  return appName;
}

function isLikelyPersonName(name = "") {
  const cleaned = cleanDisplayName(name);
  if (!cleaned) return false;
  const normalized = cleaned.toLowerCase();
  if (normalized.length < 3) return false;
  if (/\d/.test(normalized)) return false;
  if (/(recharge|bill|electric|petrol|fuel|airtel|jio|vodafone|bsnl|uber|ola|zomato|swiggy|blinkit|zepto|paytm|phonepe|gpay|bank|store|mart|market|restaurant|hotel|insurance|hospital)/.test(normalized)) {
    return false;
  }

  const parts = cleaned.split(/\s+/).filter(Boolean);
  const titleCaseParts = parts.filter(part => /^[A-Z][a-z'.-]+$/.test(part));
  return parts.length >= 2 || titleCaseParts.length >= 1;
}

function isPersonTransferContext(text = "", name = "") {
  const haystack = normalizeText(text);
  const hasP2PVerb = /(to|from|beneficiary|transferred to|sent to|paid to|received from|collected from)/.test(haystack);
  const hasMerchantSignal = /(recharge|electricity|billdesk|broadband|wifi|gas bill|water bill|petrol|fuel|zomato|swiggy|blinkit|zepto|uber|ola|netflix|spotify|amazon|flipkart)/.test(haystack);
  return isLikelyPersonName(name) && hasP2PVerb && !hasMerchantSignal;
}

function detectDirection(text = "") {
  const normalized = normalizeText(text);
  const creditWords = /(received|credited|money received|collected|has sent you|refund|cashback|deposited)/;
  const debitWords = /(paid|sent|debited|payment of|spent|charged|deducted|purchase|recharge done|bill paid)/;

  if (creditWords.test(normalized) && !debitWords.test(normalized)) return "credit";
  if (debitWords.test(normalized)) return "debit";
  if (/\bfrom\b/.test(normalized) && !/\bto\b/.test(normalized)) return "credit";
  return "debit";
}

function detectCategory(name = "", body = "") {
  const haystack = normalizeText(`${name} ${body}`);
  if (isPersonTransferContext(body, name)) {
    return "Person Transfer";
  }
  let winner = { category: "Person Transfer", score: 0 };

  for (const rule of CATEGORY_RULES) {
    let score = 0;
    for (const keyword of rule.keywords) {
      if (haystack.includes(keyword)) {
        score += keyword.length > 6 ? 3 : 2;
      }
    }
    if (score > winner.score) {
      winner = { category: rule.category, score };
    }
  }

  if (winner.score === 0) {
    if (/\b(bank|upi|transfer|beneficiary|sent to|received from)\b/.test(haystack)) {
      return "Person Transfer";
    }
    return "Others";
  }

  return winner.category;
}

function applyAlias(name, aliases = {}) {
  const key = name.toLowerCase();
  return aliases[key] || name;
}

function classifyTransactionFields(payload = {}, aliases = {}) {
  const title = payload.title || payload.rawTitle || "";
  const text = payload.text || payload.rawText || "";
  const appName = normalizeSourceApp(payload.appName || payload.sourceApp || "");
  const upiId = payload.upiId || "";
  const combined = `${title} ${text}`.trim();
  const amount = normalizeAmount(combined) || Number(payload.amount || 0);
  const direction = detectDirection(combined);

  const rawName = cleanDisplayName(
    payload.payeeName ||
      payload.rawName ||
      extractName(title) ||
      extractName(text) ||
      extractName(combined) ||
      title ||
      "Unknown"
  ) || "Unknown";

  const name = applyAlias(rawName, aliases);
  const category = direction === "credit"
    ? "Money Received"
    : isPersonTransferContext(combined, name)
      ? "Person Transfer"
      : detectCategory(name, combined);

  return {
    sourceApp: appName,
    rawTitle: title,
    rawText: text,
    upiId,
    amount,
    direction,
    rawName,
    name,
    category,
    icon: getCategoryIcon(category),
    color: getCategoryColor(category),
  };
}

function parseNotification(payload, aliases = {}) {
  return classifyTransactionFields(payload, aliases);
}

function buildSubscriptions(tx) {
  const debitMap = {};
  for (const item of tx) {
    if (item.direction !== "debit") continue;
    const key = item.name.toLowerCase();
    if (!debitMap[key]) debitMap[key] = [];
    debitMap[key].push({ d: new Date(item.createdAt), a: item.amount, i: item.icon });
  }

  const subscriptions = [];
  for (const [name, instances] of Object.entries(debitMap)) {
    if (instances.length < 2) continue;
    const months = new Set(instances.map(inst => `${inst.d.getMonth()}-${inst.d.getFullYear()}`));
    if (months.size >= 2) {
      const avg = instances.reduce((sum, inst) => sum + inst.a, 0) / instances.length;
      const sortedDates = instances.map(inst => inst.d).sort((a, b) => a - b);
      const lastDate = sortedDates[sortedDates.length - 1];
      const previousDate = sortedDates[sortedDates.length - 2];
      const diffDays = Math.max(1, Math.round((lastDate - previousDate) / (1000 * 60 * 60 * 24)));
      const nextRenewal = new Date(lastDate);
      nextRenewal.setDate(nextRenewal.getDate() + diffDays);
      const variation = instances.reduce((sum, inst) => sum + Math.abs(inst.a - avg), 0) / instances.length;
      const confidence = Math.max(0.35, Math.min(0.99, 1 - (variation / Math.max(avg, 1))));
      subscriptions.push({
        name: name.charAt(0).toUpperCase() + name.slice(1),
        avgAmount: Math.round(avg),
        count: instances.length,
        icon: instances[0].icon,
        nextRenewal: nextRenewal.toISOString(),
        confidence: Number(confidence.toFixed(2)),
        monthlyWaste: Math.round(avg),
      });
    }
  }

  return subscriptions.sort((a, b) => b.avgAmount - a.avgAmount);
}

function buildSeries(tx) {
  const monthlyMap = {};
  const yearlyMap = {};

  tx.forEach(item => {
    const date = new Date(item.createdAt);
    const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
    const yearKey = String(date.getFullYear());

    if (!monthlyMap[monthKey]) monthlyMap[monthKey] = { label: monthKey, spent: 0, received: 0 };
    if (!yearlyMap[yearKey]) yearlyMap[yearKey] = { label: yearKey, spent: 0, received: 0 };

    if (item.direction === "debit") {
      monthlyMap[monthKey].spent += item.amount;
      yearlyMap[yearKey].spent += item.amount;
    } else {
      monthlyMap[monthKey].received += item.amount;
      yearlyMap[yearKey].received += item.amount;
    }
  });

  return {
    monthlySeries: Object.values(monthlyMap).sort((a, b) => a.label.localeCompare(b.label)),
    yearlySeries: Object.values(yearlyMap).sort((a, b) => a.label.localeCompare(b.label)),
  };
}

function buildMerchantStats(tx) {
  const merchantMap = {};
  tx.forEach(item => {
    const key = item.name.toLowerCase();
    if (!merchantMap[key]) {
      merchantMap[key] = {
        name: item.name,
        count: 0,
        total: 0,
        category: item.category,
        icon: item.icon,
      };
    }
    merchantMap[key].count += 1;
    merchantMap[key].total += item.amount;
  });

  return Object.values(merchantMap)
    .sort((a, b) => (b.total === a.total ? b.count - a.count : b.total - a.total))
    .slice(0, 10);
}

function buildSmartAlerts({ budget = 0, spent = 0, tx = [], categoryTotals = {}, subscriptions = [] }) {
  const alerts = [];
  if (budget > 0) {
    const used = Math.round((spent / budget) * 100);
    if (used >= 100) {
      alerts.push({ title: "Budget exceeded", body: `You have crossed your budget by Rs ${Math.max(0, spent - budget).toLocaleString("en-IN")}.`, severity: "danger" });
    } else if (used >= 85) {
      alerts.push({ title: "Budget warning", body: `${used}% of this month's budget is already used.`, severity: "warning" });
    }
  }

  const foodSpend = categoryTotals.Food || 0;
  if (foodSpend > 0 && spent > 0 && foodSpend / spent > 0.28) {
    alerts.push({ title: "Food spend spike", body: `Food accounts for ${Math.round((foodSpend / spent) * 100)}% of your debit spend.`, severity: "info" });
  }

  const now = Date.now();
  subscriptions.slice(0, 3).forEach(sub => {
    if (!sub.nextRenewal) return;
    const daysAway = Math.round((new Date(sub.nextRenewal).getTime() - now) / (1000 * 60 * 60 * 24));
    if (daysAway >= 0 && daysAway <= 5) {
      alerts.push({ title: "Renewal coming up", body: `${sub.name} is likely due in ${daysAway} day${daysAway === 1 ? "" : "s"}.`, severity: "warning" });
    }
  });

  if (tx.length > 1) {
    const latest = tx.slice().sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt))[0];
    if (latest.direction === "debit" && latest.amount > Math.max(1500, spent * 0.18)) {
      alerts.push({ title: "Unusual spend", body: `${latest.name} at Rs ${latest.amount.toLocaleString("en-IN")} is larger than your usual recent pattern.`, severity: "info" });
    }
  }

  return alerts.slice(0, 5);
}

function buildStableId(value = "") {
  const input = String(value || "");
  let hash = 0;
  for (let index = 0; index < input.length; index += 1) {
    hash = ((hash << 5) - hash) + input.charCodeAt(index);
    hash |= 0;
  }
  return `EV${Math.abs(hash)}`;
}

async function getUserDoc() {
  const doc = await db.collection("users").doc("me").get();
  return doc.exists ? doc.data() : null;
}

async function commitInChunks(mutator) {
  const refs = [];
  await mutator(refs);
  for (let index = 0; index < refs.length; index += 400) {
    const batch = db.batch();
    refs.slice(index, index + 400).forEach(fn => fn(batch));
    await batch.commit();
  }
}

app.get("/health", (_req, res) => res.json({ ok: true }));

app.get("/api/user", async (_req, res) => {
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
      id: isNew ? `u_${Date.now()}` : current.id,
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

app.delete("/api/user/reset", async (_req, res) => {
  try {
    const collections = ["transactions", "notifications", "ledger", "raw_notifications"];
    await commitInChunks(async refs => {
      for (const collection of collections) {
        const snapshot = await db.collection(collection).get();
        snapshot.docs.forEach(doc => refs.push(batch => batch.delete(doc.ref)));
      }
      refs.push(batch => batch.set(db.collection("users").doc("me"), { balance: 0 }, { merge: true }));
    });

    res.json({ success: true, data: { reset: true } });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/api/transactions", async (_req, res) => {
  try {
    const snapshot = await db.collection("transactions").orderBy("createdAt", "desc").get();
    res.json({ success: true, data: snapshot.docs.map(doc => doc.data()) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post("/api/transactions/reclassify", async (_req, res) => {
  try {
    const userDoc = await getUserDoc();
    const aliases = userDoc?.aliases || {};
    const snapshot = await db.collection("transactions").get();
    let updated = 0;

    await commitInChunks(async refs => {
      snapshot.docs.forEach(doc => {
        const current = doc.data();
        const classified = classifyTransactionFields(current, aliases);
        refs.push(batch => batch.update(doc.ref, {
          name: classified.name,
          rawName: classified.rawName,
          category: classified.category,
          icon: classified.icon,
          color: classified.color,
          direction: current.direction || classified.direction,
          amount: current.amount || classified.amount,
        }));
        updated += 1;
      });
    });

    res.json({ success: true, data: { updated } });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/api/notifications", async (_req, res) => {
  try {
    const snapshot = await db.collection("notifications").orderBy("createdAt", "desc").get();
    res.json({ success: true, data: snapshot.docs.map(doc => doc.data()) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/api/summary", async (_req, res) => {
  try {
    const [userDoc, txSnap, ledgerSnap] = await Promise.all([
      getUserDoc(),
      db.collection("transactions").get(),
      db.collection("ledger").where("settled", "==", false).get(),
    ]);

    const tx = txSnap.docs.map(doc => doc.data());
    const spent = tx.filter(item => item.direction === "debit").reduce((sum, item) => sum + item.amount, 0);
    const received = tx.filter(item => item.direction === "credit").reduce((sum, item) => sum + item.amount, 0);

    const categoryTotals = {};
    for (const item of tx) {
      if (item.direction !== "debit") continue;
      categoryTotals[item.category] = (categoryTotals[item.category] || 0) + item.amount;
    }

    const topCategory = Object.keys(categoryTotals).length
      ? Object.keys(categoryTotals).reduce((a, b) => (categoryTotals[a] > categoryTotals[b] ? a : b))
      : "None";

    const ledger = ledgerSnap.docs.map(doc => doc.data());
    const totalLent = ledger.filter(entry => entry.type === "lent").reduce((sum, entry) => sum + entry.amount, 0);
    const totalBorrowed = ledger.filter(entry => entry.type === "borrowed").reduce((sum, entry) => sum + entry.amount, 0);
    const subscriptions = buildSubscriptions(tx);
    const { monthlySeries, yearlySeries } = buildSeries(tx);
    const merchantStats = buildMerchantStats(tx);
    const smartAlerts = buildSmartAlerts({
      budget: userDoc ? userDoc.budget : 0,
      spent,
      tx,
      categoryTotals,
      subscriptions,
    });

    res.json({
      success: true,
      data: {
        totalSpent: spent,
        totalReceived: received,
        balance: userDoc ? userDoc.balance : 0,
        budget: userDoc ? userDoc.budget : 0,
        topCategory,
        count: tx.length,
        categoryTotals,
        totalLent,
        totalBorrowed,
        subscriptions,
        smartAlerts,
        merchantStats,
        monthlySeries,
        yearlySeries,
      },
    });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post("/api/ingest-notification", async (req, res) => {
  try {
    const payload = req.body || {};
    const eventId = buildStableId(
      payload.eventId ||
      `${payload.appName || ""}|${payload.packageName || ""}|${payload.postTime || ""}|${payload.title || ""}|${payload.text || ""}`
    );
    const eventRef = db.collection("ingest_events").doc(eventId);
    const rawRef = db.collection("raw_notifications").doc(eventId);
    const userRef = db.collection("users").doc("me");

    const result = await db.runTransaction(async (txn) => {
      const [userSnap, eventSnap] = await Promise.all([
        txn.get(userRef),
        txn.get(eventRef),
      ]);

      const userDoc = userSnap.exists ? userSnap.data() : null;
      if (!userDoc) {
        throw new Error("User onboarding not completed");
      }

      if (eventSnap.exists) {
        const existing = eventSnap.data() || {};
        return { duplicate: true, txId: existing.txId || null, notificationId: existing.notificationId || null };
      }

      txn.set(rawRef, {
        ...payload,
        eventId,
        createdAt: new Date().toISOString(),
      }, { merge: true });

      const parsed = parseNotification(payload, userDoc.aliases || {});
      if (!parsed.amount || parsed.amount <= 0) {
        txn.set(eventRef, {
          eventId,
          ignored: true,
          createdAt: new Date().toISOString(),
        }, { merge: true });
        return { duplicate: false, ignored: true };
      }

      const txId = `TX${Date.now()}`;
      const tx = {
        id: txId,
        eventId,
        name: parsed.name,
        rawName: parsed.rawName,
        upiId: parsed.upiId || "",
        amount: parsed.amount,
        category: parsed.category,
        direction: parsed.direction,
        sourceApp: parsed.sourceApp,
        icon: parsed.icon,
        color: parsed.color,
        rawTitle: parsed.rawTitle,
        rawText: parsed.rawText,
        createdAt: new Date().toISOString(),
      };

      const newBalance = parsed.direction === "debit"
        ? (userDoc.balance || 0) - parsed.amount
        : (userDoc.balance || 0) + parsed.amount;

      const notificationId = `NF${Date.now()}`;
      const notification = {
        id: notificationId,
        eventId,
        title: parsed.direction === "credit" ? `Received Rs ${parsed.amount} from ${parsed.name}` : `Paid Rs ${parsed.amount} to ${parsed.name}`,
        body: `${parsed.category} | ${parsed.sourceApp}`,
        txId,
        createdAt: new Date().toISOString(),
      };

      txn.set(userRef, { balance: newBalance }, { merge: true });
      txn.set(db.collection("transactions").doc(txId), tx);
      txn.set(db.collection("notifications").doc(notificationId), notification);
      txn.set(eventRef, {
        eventId,
        txId,
        notificationId,
        createdAt: new Date().toISOString(),
      });

      return { duplicate: false, ignored: false, tx, notification };
    });

    if (result.duplicate) {
      if (result.txId && result.notificationId) {
        const [txSnap, notifSnap] = await Promise.all([
          db.collection("transactions").doc(result.txId).get(),
          db.collection("notifications").doc(result.notificationId).get(),
        ]);
        return res.json({
          success: true,
          data: {
            transaction: txSnap.exists ? txSnap.data() : null,
            notification: notifSnap.exists ? notifSnap.data() : null,
            duplicate: true,
          },
        });
      }
      return res.status(200).json({ success: true, data: null });
    }

    if (result.ignored) {
      return res.status(200).json({ success: true, data: null });
    }

    res.json({ success: true, data: { transaction: result.tx, notification: result.notification } });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/api/aliases", async (_req, res) => {
  const userDoc = await getUserDoc();
  res.json({ success: true, data: userDoc ? userDoc.aliases || {} : {} });
});

app.post("/api/alias", async (req, res) => {
  try {
    const { originalName, alias } = req.body || {};
    if (!originalName || !alias) {
      return res.status(400).json({ success: false, error: "originalName and alias required" });
    }

    const key = originalName.toLowerCase();
    const userDoc = await getUserDoc();
    const aliases = { ...(userDoc?.aliases || {}), [key]: alias };
    await db.collection("users").doc("me").set({ aliases }, { merge: true });

    const snapshot = await db.collection("transactions").get();
    await commitInChunks(async refs => {
      snapshot.docs.forEach(doc => {
        const tx = doc.data();
        if (tx.rawName && tx.rawName.toLowerCase() === key) {
          refs.push(batch => batch.update(doc.ref, { name: alias }));
        }
      });
    });

    res.json({ success: true, data: { saved: true } });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.delete("/api/alias/:name", async (req, res) => {
  try {
    const key = decodeURIComponent(req.params.name).toLowerCase();
    await db.collection("users").doc("me").update({ [`aliases.${key}`]: admin.firestore.FieldValue.delete() });
    res.json({ success: true, data: { deleted: true } });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post("/api/contacts", async (req, res) => {
  try {
    const { contacts } = req.body || {};
    if (!contacts) {
      return res.status(400).json({ success: false, error: "contacts required" });
    }
    await db.collection("users").doc("me").set({ contacts }, { merge: true });
    res.json({ success: true, data: { count: Object.keys(contacts).length } });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/api/contacts", async (_req, res) => {
  const userDoc = await getUserDoc();
  res.json({ success: true, data: userDoc ? userDoc.contacts || {} : {} });
});

app.get("/api/ledger", async (_req, res) => {
  try {
    const snapshot = await db.collection("ledger").orderBy("createdAt", "desc").get();
    res.json({ success: true, data: snapshot.docs.map(doc => doc.data()) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post("/api/ledger", async (req, res) => {
  try {
    const { personName, amount, type, note } = req.body || {};
    if (!personName || !amount || !type) {
      return res.status(400).json({ success: false, error: "personName, amount, type required" });
    }

    const entry = {
      id: `LD${Date.now()}`,
      personName: personName.trim(),
      amount: Number(amount),
      type,
      note: (note || "").trim(),
      settled: false,
      createdAt: new Date().toISOString(),
      settledAt: null,
    };

    await db.collection("ledger").doc(entry.id).set(entry);
    res.json({ success: true, data: entry });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.put("/api/ledger/:id", async (req, res) => {
  try {
    const ref = db.collection("ledger").doc(req.params.id);
    const doc = await ref.get();
    if (!doc.exists) {
      return res.status(404).json({ success: false, error: "Ledger entry not found" });
    }

    const { settled, personName, amount, note } = req.body || {};
    const updates = {};

    if (settled !== undefined) {
      updates.settled = Boolean(settled);
      updates.settledAt = updates.settled ? new Date().toISOString() : null;
    }
    if (personName) updates.personName = personName.trim();
    if (amount) updates.amount = Number(amount);
    if (note !== undefined) updates.note = note.trim();

    await ref.update(updates);
    const updated = await ref.get();
    res.json({ success: true, data: updated.data() });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.delete("/api/ledger/:id", async (req, res) => {
  try {
    await db.collection("ledger").doc(req.params.id).delete();
    res.json({ success: true, data: { deleted: true } });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/api/raw-notifications", async (_req, res) => {
  try {
    const snapshot = await db.collection("raw_notifications").orderBy("createdAt", "desc").limit(100).get();
    res.json({ success: true, data: snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() })) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.listen(PORT, () => console.log(`FlowLedger backend deployed to FireBase! On Port: ${PORT}`));
