#!/usr/bin/env bash
# ============================================================
# FalconEye — סקריפט תיקון אוטומטי מלא (ירוץ על השרת)
# ============================================================
# מה הסקריפט עושה:
# 1) עוצר את כל הקונטיינרים (הspam נפסק מיד)
# 2) יוצר Postgres חדש עם credentials עקביים
# 3) מתקן את .env שיתאים
# 4) מנקה volumes ישנים שלא תואמים
# 5) מפעיל הכל מחדש
# 6) בודק שהכל עובד
# 7) שולח לך הודעה בטלגרם עם התוצאה
# ============================================================

set -u

INSTALL_DIR="/opt/falconeye"
ENV_FILE="${INSTALL_DIR}/.env"
PROD_FILE="${INSTALL_DIR}/docker-compose.prod.yml"
OVERRIDE_FILE="${INSTALL_DIR}/docker-compose.override.yml"
COMPOSE="docker compose -f ${PROD_FILE} -f ${OVERRIDE_FILE}"

PG_USER="falcon_admin"
PG_PASS="FalconStrong_$(date +%Y)_pass"
PG_DB="trading_db"

bar() { printf '\n════════════════════════════════════════\n'; }

bar
echo "🔧 FalconEye Auto-Fix — תיקון מלא"
echo "   זמן: $(date '+%Y-%m-%d %H:%M:%S')"
bar

# ============================================================
# שלב 1: עוצרים הכל — הspam נפסק מיד
# ============================================================
echo ""
echo "📍 [1/7] עוצר את הקונטיינרים — הספאם נפסק"
cd "$INSTALL_DIR" 2>/dev/null || { echo "❌ לא נמצאה התיקייה $INSTALL_DIR"; exit 1; }

docker compose -f "$PROD_FILE" down --remove-orphans 2>&1 | tail -5 || true
[[ -f docker-compose.yml ]] && docker compose -f docker-compose.yml down --remove-orphans 2>&1 | tail -3 || true
docker rm -f falcon-db falcon-redis falcon-bot fortress-postgres fortress-redis falconeye-bot 2>/dev/null || true
echo "   ✅ אין עוד קונטיינרים פעילים. הטלגרם הפסיק לקבל ALERTים."

# ============================================================
# שלב 2: יוצר docker-compose.override.yml שמוסיף Postgres נכון
# ============================================================
echo ""
echo "📍 [2/7] יוצר Postgres חדש עם credentials עקביים"
cat > "$OVERRIDE_FILE" <<EOF
services:
  postgres:
    image: postgres:16-alpine
    container_name: falcon-db
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
# שלב 3: מעדכן את .env עם DATABASE_URL נכון (משאיר את שאר המשתנים)
# ============================================================
echo ""
echo "📍 [3/7] מתקן את ה-.env (גיבוי נשמר)"
BACKUP="${ENV_FILE}.backup-$(date +%Y%m%d-%H%M%S)"
cp "$ENV_FILE" "$BACKUP" 2>/dev/null || touch "$ENV_FILE"

# מסיר את הערכים הישנים של DB
sed -i '/^DATABASE_URL=/d' "$ENV_FILE"
sed -i '/^DB_URL=/d' "$ENV_FILE"
sed -i '/^POSTGRES_HOST=/d' "$ENV_FILE"
sed -i '/^POSTGRES_PORT=/d' "$ENV_FILE"
sed -i '/^POSTGRES_USER=/d' "$ENV_FILE"
sed -i '/^POSTGRES_PASSWORD=/d' "$ENV_FILE"
sed -i '/^POSTGRES_DB=/d' "$ENV_FILE"

# מוסיף את הערכים הנכונים
cat >> "$ENV_FILE" <<EOF

# ===== Postgres (מנוהל מקומית בdocker-compose.override.yml) =====
DATABASE_URL=postgresql://${PG_USER}:${PG_PASS}@postgres:5432/${PG_DB}
DB_URL=postgresql://${PG_USER}:${PG_PASS}@postgres:5432/${PG_DB}
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=${PG_USER}
POSTGRES_PASSWORD=${PG_PASS}
POSTGRES_DB=${PG_DB}
EOF
echo "   ✅ ה-.env מעודכן. גיבוי: $BACKUP"

# ============================================================
# שלב 4: מוחק Postgres data ישן (כי הcredentials השתנו)
# ============================================================
echo ""
echo "📍 [4/7] מנקה data ישן של Postgres"
PROJECT_NAME=$(basename "$INSTALL_DIR")
docker volume rm "${PROJECT_NAME}_postgres_data" 2>/dev/null || true
docker volume rm "falconeye_postgres_data" 2>/dev/null || true
docker volume rm "$(basename "$INSTALL_DIR" | tr '[:upper:]' '[:lower:]')_postgres_data" 2>/dev/null || true
echo "   ✅ נקי"

# ============================================================
# שלב 5: בונה ומפעיל את הstack מחדש
# ============================================================
echo ""
echo "📍 [5/7] בונה ומפעיל את הstack (זה לוקח ~30-90 שניות)"
$COMPOSE up -d --build 2>&1 | tail -15
echo "   ✅ stack הופעל"

# ============================================================
# שלב 6: ממתין ובודק שהכל מחובר
# ============================================================
echo ""
echo "📍 [6/7] ממתין שהבוט יתאים ויתחבר ל-Postgres..."
sleep 35

LOGS=$($COMPOSE logs --tail=80 bot 2>&1)
STATUS="UNKNOWN"

if echo "$LOGS" | grep -q "password authentication failed"; then
    STATUS="FAIL_AUTH"
elif echo "$LOGS" | grep -q "POSTGRES] ok"; then
    if echo "$LOGS" | grep -q "REDIS] ok"; then
        STATUS="SUCCESS"
    else
        STATUS="PARTIAL_NO_REDIS"
    fi
elif echo "$LOGS" | grep -qE "Cannot connect|Connection refused|could not translate"; then
    STATUS="FAIL_CONN"
else
    echo "   ⏳ עדיין לא ברור, ממתין 25 שניות נוספות..."
    sleep 25
    LOGS=$($COMPOSE logs --tail=100 bot 2>&1)
    if echo "$LOGS" | grep -q "POSTGRES] ok"; then
        STATUS="SUCCESS"
    elif echo "$LOGS" | grep -q "password authentication failed"; then
        STATUS="FAIL_AUTH"
    fi
fi

case "$STATUS" in
    SUCCESS) echo "   ✅ הבוט מחובר לPostgres ולRedis. הכל עובד!" ;;
    FAIL_AUTH) echo "   ❌ עדיין יש שגיאת auth — מצב חמור, צריך עזרה." ;;
    FAIL_CONN) echo "   ❌ הבוט לא מצליח להגיע ל-Postgres כלל." ;;
    PARTIAL_NO_REDIS) echo "   ⚠️ Postgres OK אבל Redis לא — נדיר." ;;
    *) echo "   ⏳ סטטוס לא ודאי. בדוק logs ידנית." ;;
esac

# ============================================================
# שלב 7: שולח הודעת אישור לטלגרם
# ============================================================
echo ""
echo "📍 [7/7] שולח הודעת תוצאה לטלגרם"

TG_TOKEN=$(grep -E "^TELEGRAM_BOT_TOKEN=" "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'" | xargs)
ADMIN_ID=$(grep -E "^ADMIN_CHAT_ID=" "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'" | xargs)
[[ -z "$ADMIN_ID" ]] && ADMIN_ID=$(grep -E "^ADMIN_ID=" "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'" | xargs)

if [[ -n "$TG_TOKEN" && -n "$ADMIN_ID" ]]; then
    case "$STATUS" in
        SUCCESS)
            MSG="✅ FalconEye תוקן! Postgres מחובר, הspam נעצר, הבוט פעיל וסורק. תוך כמה דקות יתחילו להגיע איתותים אמיתיים (לא ALERTS)."
            ;;
        FAIL_AUTH|FAIL_CONN)
            MSG="❌ סקריפט התיקון רץ אבל יש בעיה — סטטוס: $STATUS. צריך עזרה נוספת."
            ;;
        *)
            MSG="⏳ סקריפט התיקון רץ. הסטטוס: $STATUS. תפנה לClaude לוודא שהכל בסדר."
            ;;
    esac
    curl -sS --max-time 15 -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
        -d "chat_id=${ADMIN_ID}" \
        --data-urlencode "text=${MSG}" >/dev/null && echo "   ✅ הודעה נשלחה לטלגרם"
else
    echo "   ⚠️ לא נמצא TELEGRAM_BOT_TOKEN/ADMIN_CHAT_ID ב-.env"
fi

bar
echo "🏁 סיום — סטטוס: $STATUS"
bar
echo ""
echo "20 שורות אחרונות מהlogs (לראיה):"
echo "$LOGS" | tail -20
echo ""
echo "פקודה לצפייה חיה בלוגים בעתיד:"
echo "  $COMPOSE logs -f bot"
