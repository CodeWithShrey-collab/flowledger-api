require("dotenv").config();
const admin = require("firebase-admin");
const fs = require("fs");
const path = require("path");

const serviceAccountKey = process.env.FIREBASE_SERVICE_ACCOUNT 
  ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT) 
  : require("./firebase-service-account.json");

admin.initializeApp({ credential: admin.credential.cert(serviceAccountKey) });
const db = admin.firestore();

async function run() {
  const dbPath = path.join(__dirname, "data", "db.json");
  if (!fs.existsSync(dbPath)) {
    console.log("No local data found. Skipping migration.");
    return;
  }

  const localDb = JSON.parse(fs.readFileSync(dbPath, "utf-8"));
  
  if (localDb.user) {
    await db.collection("users").doc("me").set({
      ...localDb.user,
      aliases: localDb.aliases || {},
      contacts: localDb.contacts || {}
    });
    console.log("Migrated user data & aliases.");
  }

  const txs = localDb.transactions || [];
  for (const t of txs) {
    await db.collection("transactions").doc(t.id).set(t);
  }
  console.log(`Migrated ${txs.length} transactions.`);

  const notes = localDb.notifications || [];
  for (const n of notes) {
    await db.collection("notifications").doc(n.id).set(n);
  }
  console.log(`Migrated ${notes.length} notifications.`);

  const lgr = localDb.ledger || [];
  for (const L of lgr) {
    await db.collection("ledger").doc(L.id).set(L);
  }
  console.log(`Migrated ${lgr.length} ledger entries.`);

  console.log("🎉 Migration complete!");
  process.exit(0);
}

run().catch(console.error);
