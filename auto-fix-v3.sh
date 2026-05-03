#!/usr/bin/env bash
# ============================================================
# FalconEye Auto-Fix V3 — RESET מלא + שחזור .env מ-backup
# ============================================================
# מה שונה מ-V2:
#  - הריגת volumes לחלוטין (לא רק postgres_data)
#  - שחזור .env מהbackup הראשון לפני כל השינויים
#  - וידוא ש-TELEGRAM_BOT_TOKEN שורד את התיקון
#  - הוספת DATABASE_URL בזהירות (תוך כדי source bash)
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
echo "🔧 FalconEye Auto-Fix V3 — RESET מלא"
echo "   זמן: $(date '+%Y-%m-%d %H:%M:%S')"
bar

cd "$INSTALL_DIR" 2>/dev/null || { echo "❌ /opt/falconeye לא קיים"; exit 1; }

# ============================================================
# שלב 0: שחזור .env מהbackup הראשון לפני שעשינו דברים
# ============================================================
echo ""
echo "📍 [0/8] משחזר .env מbackup הראשון"
EARLIEST_BACKUP=$(ls -1 ${ENV_FILE}.backup-* 2>/dev/null | sort | head -1)
if [[ -n "$EARLIEST_BACKUP" ]]; then
    echo "   נמצא backup ראשון: $EARLIEST_BACKUP"
    cp "$EARLIEST_BACKUP" "$ENV_FILE"
    echo "   ✅ .env שוחזר"
else
    echo "   ⚠️ לא נמצא backup. ממשיך עם .env הנוכחי."
fi

# וידוא שטוקן הטלגרם קיים
if grep -q "^TELEGRAM_BOT_TOKEN=" "$ENV_FILE"; then
    echo "   ✅ TELEGRAM_BOT_TOKEN קיים ב-.env"
else
    echo "   ❌ TELEGRAM_BOT_TOKEN חסר! עוצר — צריך לטפל ידנית."
    exit 1
fi

# ============================================================
# שלב 1: הריגת הכל בלי רחמים — קונטיינרים, volumes, networks
# ============================================================
echo ""
echo "📍 [1/8] הריגת הכל — clean slate מוחלט"
ALL_CONTAINERS=$(docker ps -aq 2>/dev/null)
[[ -n "$ALL_CONTAINERS" ]] && {
    docker stop $ALL_CONTAINERS 2>/dev/null >/dev/null
    docker rm -f $ALL_CONTAINERS 2>/dev/null >/dev/null
}
# מחיקה של כל הvolumes שקשורים לפרויקט
docker volume ls --format '{{.Name}}' 2>/dev/null | grep -iE "postgres|redis|falcon|fortress" | while read v; do
    docker volume rm -f "$v" 2>/dev/null
done
docker network prune -f 2>/dev/null >/dev/null
echo "   ✅ הכל נמחק. clean slate."

# ============================================================
# שלב 2: כתיבת docker-compose.override.yml מינימלי
# ============================================================
echo ""
echo "📍 [2/8] כותב override.yml"
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
      - falcon_pg_v3:/var/lib/postgresql/data
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
  falcon_pg_v3:
EOF
echo "   ✅ נוצר $OVERRIDE_FILE"

# ============================================================
# שלב 3: הוספת DATABASE_URL ל-.env (בזהירות, לא דורסים כלום)
# ============================================================
echo ""
echo "📍 [3/8] מעדכן DATABASE_URL ב-.env"
# מסיר את הערכים הישנים של DB
for key in DATABASE_URL DB_URL POSTGRES_HOST POSTGRES_PORT POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB; do
    sed -i "/^${key}=/d" "$ENV_FILE"
done

# מוודא שיש newline בסוף לפני הappend
tail -c1 "$ENV_FILE" | read -r _ || echo "" >> "$ENV_FILE"

cat >> "$ENV_FILE" <<EOF

# ===== Postgres (auto-fix v3) =====
DATABASE_URL=postgresql://${PG_USER}:${PG_PASS}@postgres:5432/${PG_DB}
DB_URL=postgresql://${PG_USER}:${PG_PASS}@postgres:5432/${PG_DB}
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=${PG_USER}
POSTGRES_PASSWORD=${PG_PASS}
POSTGRES_DB=${PG_DB}
EOF
echo "   ✅ DATABASE_URL הוספו"

# ============================================================
# שלב 4: וידוא שהtoken עדיין שם
# ============================================================
echo ""
echo "📍 [4/8] בודק שכל המשתנים החשובים קיימים"
for key in TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID DATABASE_URL POSTGRES_USER; do
    if grep -q "^${key}=" "$ENV_FILE"; then
        echo "   ✅ $key קיים"
    else
        echo "   ❌ $key חסר!"
    fi
done

# ============================================================
# שלב 5: בנייה והפעלה
# ============================================================
echo ""
echo "📍 [5/8] בונה ומפעיל את הstack"
docker compose -f "$PROD_FILE" -f "$OVERRIDE_FILE" up -d --build 2>&1 | tail -20

sleep 5
echo ""
echo "   קונטיינרים שרצים:"
docker ps --format 'table {{.Names}}\t{{.Status}}' | head -10

# ============================================================
# שלב 6: ממתין לחיבור
# ============================================================
echo ""
echo "📍 [6/8] ממתין 50 שניות לחיבור..."
sleep 50

LOGS=$(docker compose -f "$PROD_FILE" -f "$OVERRIDE_FILE" logs --tail=80 bot 2>&1)
STATUS="UNKNOWN"

if echo "$LOGS" | grep -q "password authentication failed"; then
    STATUS="FAIL_AUTH"
elif echo "$LOGS" | grep -q "POSTGRES] ok"; then
    STATUS="SUCCESS"
elif echo "$LOGS" | grep -qE "Cannot connect|Connection refused|could not translate|Name or service not known"; then
    STATUS="FAIL_CONN"
elif [[ -z "$LOGS" || $(echo "$LOGS" | wc -l) -lt 5 ]]; then
    STATUS="NO_LOGS"
fi

[[ "$STATUS" == "UNKNOWN" || "$STATUS" == "NO_LOGS" ]] && {
    echo "   ⏳ עוד 30 שניות..."
    sleep 30
    LOGS=$(docker compose -f "$PROD_FILE" -f "$OVERRIDE_FILE" logs --tail=120 bot 2>&1)
    if echo "$LOGS" | grep -q "POSTGRES] ok"; then STATUS="SUCCESS"
    elif echo "$LOGS" | grep -q "password authentication failed"; then STATUS="FAIL_AUTH"
    fi
}

case "$STATUS" in
    SUCCESS) echo "   ✅ הבוט מחובר ל-Postgres! הצלחה!" ;;
    FAIL_AUTH) echo "   ❌ עדיין auth failure" ;;
    FAIL_CONN) echo "   ❌ הבוט לא מצליח להגיע ל-Postgres" ;;
    NO_LOGS) echo "   ❌ אין logs בכלל - הbot לא עלה" ;;
    *) echo "   ⏳ סטטוס: $STATUS" ;;
esac

# ============================================================
# שלב 7: שולח הודעה לטלגרם (משתמש ב-source bash, הכי אמין)
# ============================================================
echo ""
echo "📍 [7/8] שולח הודעה לטלגרם"
set -a
source "$ENV_FILE" 2>/dev/null
set +a

ADMIN_TARGET="${ADMIN_CHAT_ID:-${ADMIN_ID:-${TELEGRAM_CHAT_ID:-}}}"

if [[ -n "${TELEGRAM_BOT_TOKEN:-}" && -n "$ADMIN_TARGET" ]]; then
    case "$STATUS" in
        SUCCESS) MSG="✅ FalconEye V3 — Postgres מחובר, הspam פסק. הבוט סורק עכשיו." ;;
        *) MSG="⚠️ V3 רץ — סטטוס: ${STATUS}. צריך עזרה נוספת." ;;
    esac
    R=$(curl -sS --max-time 15 -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d "chat_id=${ADMIN_TARGET}" \
        --data-urlencode "text=${MSG}" 2>&1)
    if echo "$R" | grep -q '"ok":true'; then
        echo "   ✅ הודעה נשלחה לטלגרם"
    else
        echo "   ⚠️ Telegram דחה: $R"
    fi
else
    echo "   ⚠️ token=${TELEGRAM_BOT_TOKEN:-MISSING} admin=${ADMIN_TARGET:-MISSING}"
fi

# ============================================================
# שלב 8: סיכום
# ============================================================
bar
echo "🏁 סיום — סטטוס: $STATUS"
bar
echo ""
echo "30 שורות אחרונות מהbot logs:"
echo "$LOGS" | tail -30
echo ""
echo "מצב הקונטיינרים:"
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | head -10
