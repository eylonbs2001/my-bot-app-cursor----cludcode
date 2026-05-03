#!/usr/bin/env bash
# ============================================================
# FalconEye — Auto-Fix V2 — תיקון אגרסיבי וסופי
# ============================================================
# מתקן את הבעיות מהריצה הקודמת:
#  - קונטיינרים תקועים שלא נמחקו
#  - parsing של .env שנכשל
#  - container_name conflict
# ============================================================

set -u

INSTALL_DIR="/opt/falconeye"
ENV_FILE="${INSTALL_DIR}/.env"
PROD_FILE="${INSTALL_DIR}/docker-compose.prod.yml"
OVERRIDE_FILE="${INSTALL_DIR}/docker-compose.override.yml"

PG_USER="falcon_admin"
PG_PASS="FalconStrong2026"
PG_DB="trading_db"

bar() { printf '\n════════════════════════════════════════\n'; }

bar
echo "🔧 FalconEye Auto-Fix V2"
echo "   זמן: $(date '+%Y-%m-%d %H:%M:%S')"
bar

cd "$INSTALL_DIR" 2>/dev/null || { echo "❌ /opt/falconeye לא קיים"; exit 1; }

# ============================================================
# שלב 1: הריגה אגרסיבית של כל הקונטיינרים (כל מי שקיים)
# ============================================================
echo ""
echo "📍 [1/7] הורג את כל הקונטיינרים — clean slate"
ALL=$(docker ps -aq 2>/dev/null)
if [[ -n "$ALL" ]]; then
    docker stop $ALL 2>/dev/null >/dev/null
    docker rm -f $ALL 2>/dev/null >/dev/null
fi
docker network prune -f 2>/dev/null >/dev/null
echo "   ✅ כל הקונטיינרים מתו. הspam נעצר."

# ============================================================
# שלב 2: יוצר override.yml מינימלי שמוסיף Postgres
# ============================================================
echo ""
echo "📍 [2/7] כותב docker-compose.override.yml חדש"
rm -f "$OVERRIDE_FILE"
cat > "$OVERRIDE_FILE" <<EOF
services:
  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASS}
      POSTGRES_DB: ${PG_DB}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER} -d ${PG_DB}"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
    networks:
      - falcon_net

  bot:
    depends_on:
      redis:
        condition: service_healthy
      postgres:
        condition: service_healthy

volumes:
  postgres_data:
EOF
echo "   ✅ נוצר $OVERRIDE_FILE"

# ============================================================
# שלב 3: מתקן את .env (בלי לשבור משתנים אחרים)
# ============================================================
echo ""
echo "📍 [3/7] מעדכן .env — מסיר ערכי DB ישנים, מוסיף חדשים"
[[ -f "$ENV_FILE" ]] && cp "$ENV_FILE" "${ENV_FILE}.backup-v2-$(date +%Y%m%d-%H%M%S)"

python3 - <<PY
import os, re
path = "${ENV_FILE}"
new_db_lines = """
# ===== Postgres (auto-fix v2) =====
DATABASE_URL=postgresql://${PG_USER}:${PG_PASS}@postgres:5432/${PG_DB}
DB_URL=postgresql://${PG_USER}:${PG_PASS}@postgres:5432/${PG_DB}
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=${PG_USER}
POSTGRES_PASSWORD=${PG_PASS}
POSTGRES_DB=${PG_DB}
""".strip()

remove_keys = {"DATABASE_URL", "DB_URL", "POSTGRES_HOST", "POSTGRES_PORT",
               "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"}

if not os.path.exists(path):
    print("⚠️  .env לא קיים — יוצר חדש"); content_lines = []
else:
    with open(path, encoding="utf-8") as f:
        content_lines = f.read().splitlines()

kept = []
for line in content_lines:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        kept.append(line); continue
    m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=", stripped)
    if m and m.group(1) in remove_keys:
        continue
    kept.append(line)

# remove old "Postgres (auto-fix..." sections
filtered = []
skip_block = False
for line in kept:
    if "Postgres (auto-fix" in line:
        skip_block = True; continue
    if skip_block:
        if line.strip().startswith("#") or line.strip().startswith(("DATABASE_URL","DB_URL","POSTGRES_")):
            continue
        skip_block = False
    filtered.append(line)

# trim trailing empty lines
while filtered and not filtered[-1].strip():
    filtered.pop()

final = "\n".join(filtered) + "\n\n" + new_db_lines + "\n"
with open(path, "w", encoding="utf-8") as f:
    f.write(final)
print("   ✅ .env מעודכן")
PY

# ============================================================
# שלב 4: מנקה Postgres volume ישן (סיסמה משתנה דורשת data חדש)
# ============================================================
echo ""
echo "📍 [4/7] מנקה Postgres volume ישן"
docker volume rm falconeye_postgres_data 2>/dev/null || true
docker volume rm "$(basename "$INSTALL_DIR")_postgres_data" 2>/dev/null || true
docker volume ls --format '{{.Name}}' | grep -E "postgres_data" | while read v; do
    docker volume rm "$v" 2>/dev/null || true
done
echo "   ✅ נקי"

# ============================================================
# שלב 5: בונה ומפעיל
# ============================================================
echo ""
echo "📍 [5/7] בונה ומפעיל את הstack (~60-90 שניות)"
docker compose -f "$PROD_FILE" -f "$OVERRIDE_FILE" up -d --build 2>&1 | tail -20
sleep 5
echo ""
echo "   רשימת קונטיינרים שרצים עכשיו:"
docker ps --format 'table {{.Names}}\t{{.Status}}' | head -10

# ============================================================
# שלב 6: ממתין ובודק
# ============================================================
echo ""
echo "📍 [6/7] ממתין 40 שניות שהבוט יתחבר ל-Postgres..."
sleep 40

LOGS=$(docker compose -f "$PROD_FILE" -f "$OVERRIDE_FILE" logs --tail=80 bot 2>&1)
STATUS="UNKNOWN"

if echo "$LOGS" | grep -q "password authentication failed"; then
    STATUS="FAIL_AUTH"
elif echo "$LOGS" | grep -q "POSTGRES] ok"; then
    STATUS="SUCCESS"
elif echo "$LOGS" | grep -qE "Cannot connect|Connection refused|could not translate|Name or service not known"; then
    STATUS="FAIL_CONN"
fi

[[ "$STATUS" == "UNKNOWN" ]] && {
    echo "   ⏳ עוד 25 שניות..."
    sleep 25
    LOGS=$(docker compose -f "$PROD_FILE" -f "$OVERRIDE_FILE" logs --tail=120 bot 2>&1)
    if echo "$LOGS" | grep -q "POSTGRES] ok"; then STATUS="SUCCESS";
    elif echo "$LOGS" | grep -q "password authentication failed"; then STATUS="FAIL_AUTH"; fi
}

case "$STATUS" in
    SUCCESS) echo "   ✅ הבוט מחובר ל-Postgres! הכל עובד." ;;
    FAIL_AUTH) echo "   ❌ עדיין auth failure. צריך עזרה ידנית." ;;
    FAIL_CONN) echo "   ❌ הבוט לא מצליח להגיע ל-Postgres." ;;
    *) echo "   ⏳ סטטוס לא ודאי — בוא נראה logs." ;;
esac

# ============================================================
# שלב 7: שולח הודעה לטלגרם (parsing חזק יותר)
# ============================================================
echo ""
echo "📍 [7/7] שולח הודעה לטלגרם"

TG_TOKEN=$(python3 -c "
import re
try:
    with open('${ENV_FILE}', encoding='utf-8') as f:
        for line in f:
            m = re.match(r'^\s*TELEGRAM_BOT_TOKEN\s*=\s*[\"\\']?([^\"\\'\s#]+)', line)
            if m: print(m.group(1)); break
except: pass
" 2>/dev/null)

ADMIN_ID=$(python3 -c "
import re
try:
    with open('${ENV_FILE}', encoding='utf-8') as f:
        c = f.read()
    for key in ('ADMIN_CHAT_ID','ADMIN_ID','TELEGRAM_CHAT_ID'):
        m = re.search(rf'^\s*{key}\s*=\s*[\"\\']?(-?\d+)', c, re.M)
        if m: print(m.group(1)); break
except: pass
" 2>/dev/null)

if [[ -n "$TG_TOKEN" && -n "$ADMIN_ID" ]]; then
    case "$STATUS" in
        SUCCESS)
            MSG="✅ FalconEye תוקן בגרסה 2! Postgres מחובר, הspam נעצר. תוך כמה דקות יתחילו איתותים אמיתיים."
            ;;
        *)
            MSG="⚠️ סקריפט V2 רץ, סטטוס: ${STATUS}. תפנה לClaude."
            ;;
    esac
    R=$(curl -sS --max-time 15 -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
        -d "chat_id=${ADMIN_ID}" \
        --data-urlencode "text=${MSG}" 2>&1)
    if echo "$R" | grep -q '"ok":true'; then
        echo "   ✅ הודעה נשלחה"
    else
        echo "   ⚠️ Telegram דחה — תגובה: $R"
    fi
else
    echo "   ⚠️ TELEGRAM_BOT_TOKEN או ADMIN_ID לא נמצאו ב-.env"
    echo "   debug — מילים שחיפשתי:"
    grep -E "^(TELEGRAM_BOT_TOKEN|ADMIN_CHAT_ID|ADMIN_ID|TELEGRAM_CHAT_ID)" "$ENV_FILE" | sed 's/=.*/=***/'
fi

bar
echo "🏁 סיום — סטטוס: $STATUS"
bar
echo ""
echo "30 שורות אחרונות מהlogs:"
echo "$LOGS" | tail -30
