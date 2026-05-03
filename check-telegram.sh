#!/usr/bin/env bash
# בדיקת Telegram — מריץ את כל הבדיקות אוטומטית ומדפיס תוצאה ברורה.
# שימוש:  ./check-telegram.sh

set -u

cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
    echo "❌ לא נמצא קובץ .env בתיקייה הזאת."
    echo "   צריך להריץ את הסקריפט מתוך תיקיית הפרויקט (where scanner.py lives)."
    exit 1
fi

# טעינת משתני הסביבה מקובץ .env
set -a
# shellcheck disable=SC1091
source .env
set +a

PASS=0
FAIL=0

separator() { printf '\n────────────────────────────────────────\n'; }

check_var() {
    local name=$1
    local val=${!name:-}
    if [[ -z $val ]]; then
        echo "❌ המשתנה $name לא מוגדר ב-.env"
        FAIL=$((FAIL+1))
        return 1
    fi
    return 0
}

separator
echo "🔍 בדיקה 1/4 — האם משתני הסביבה קיימים?"
check_var TELEGRAM_BOT_TOKEN && echo "✅ TELEGRAM_BOT_TOKEN קיים"
check_var TELEGRAM_CHAT_ID  && echo "✅ TELEGRAM_CHAT_ID קיים: $TELEGRAM_CHAT_ID"
check_var VIP_PLUS_CHAT_ID  && echo "✅ VIP_PLUS_CHAT_ID קיים: $VIP_PLUS_CHAT_ID"

if [[ -z ${TELEGRAM_BOT_TOKEN:-} ]]; then
    echo
    echo "🛑 חסר token. תוודא שיש שורה כזאת ב-.env (בלי רווחים סביב ה-=):"
    echo "   TELEGRAM_BOT_TOKEN=123456:ABC...your-token..."
    exit 1
fi

separator
echo "🔍 בדיקה 2/4 — האם ה-token תקף? (getMe)"
ME=$(curl -sS --max-time 15 "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getMe")
if echo "$ME" | grep -q '"ok":true'; then
    USERNAME=$(echo "$ME" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['username'])" 2>/dev/null || echo "?")
    echo "✅ Token תקף — שם הבוט: @${USERNAME}"
    PASS=$((PASS+1))
else
    echo "❌ Token לא תקף או נחסם. תגובת Telegram:"
    echo "$ME" | python3 -m json.tool 2>/dev/null || echo "$ME"
    echo
    echo "🛑 פתרון: היכנס ל-@BotFather בטלגרם → /mybots → בחר את הבוט → API Token → תייצר חדש."
    exit 1
fi

separator
echo "🔍 בדיקה 3/4 — האם הבוט יכול לשלוח לערוץ הראשי? (TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID)"
RESP=$(curl -sS --max-time 15 -X POST \
    "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d "chat_id=${TELEGRAM_CHAT_ID}" \
    -d "text=🧪 בדיקת חיבור מהמחשב — $(date '+%H:%M:%S')")
if echo "$RESP" | grep -q '"ok":true'; then
    echo "✅ נשלחה הודעה לערוץ הראשי. תפתח את הטלגרם ותראה אותה."
    PASS=$((PASS+1))
else
    echo "❌ השליחה נכשלה. תגובת Telegram:"
    echo "$RESP" | python3 -m json.tool 2>/dev/null || echo "$RESP"
    DESC=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('description',''))" 2>/dev/null || echo "")
    echo
    case "$DESC" in
        *"chat not found"*)
            echo "🛑 ה-chat_id לא נכון. שלח הודעה כלשהי לערוץ ואז תפתח:"
            echo "   https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates"
            echo "   ותעתיק את chat.id משם ל-.env."
            ;;
        *"bot was kicked"*|*"not enough rights"*|*"need administrator"*|*"not a member"*)
            echo "🛑 הבוט לא admin בערוץ. תיכנס לערוץ → Manage → Administrators → Add Admin → תוסיף את @${USERNAME}"
            echo "   ותסמן 'Post Messages'."
            ;;
        *"Forbidden"*)
            echo "🛑 הבוט חסום בערוץ. תוסיף אותו מחדש כadmin."
            ;;
        *)
            echo "🛑 שגיאה לא מוכרת. שלח לי את הJSON למעלה."
            ;;
    esac
    FAIL=$((FAIL+1))
fi

separator
echo "🔍 בדיקה 4/4 — האם הבוט יכול לשלוח לערוץ VIP+? (VIP_PLUS_CHAT_ID=$VIP_PLUS_CHAT_ID)"
RESP=$(curl -sS --max-time 15 -X POST \
    "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d "chat_id=${VIP_PLUS_CHAT_ID}" \
    -d "text=🧪 בדיקת VIP+ מהמחשב — $(date '+%H:%M:%S')")
if echo "$RESP" | grep -q '"ok":true'; then
    echo "✅ נשלחה הודעה לערוץ VIP+. תפתח את הטלגרם ותראה אותה."
    PASS=$((PASS+1))
else
    echo "❌ השליחה לערוץ VIP+ נכשלה. תגובת Telegram:"
    echo "$RESP" | python3 -m json.tool 2>/dev/null || echo "$RESP"
    echo
    echo "🛑 רוב הסיכויים שהבוט לא admin בערוץ ה-VIP+. תוסיף אותו שם בדיוק כמו לערוץ הראשי."
    FAIL=$((FAIL+1))
fi

separator
echo "📊 סיכום: $PASS עברו | $FAIL נכשלו"
echo
if [[ $FAIL -eq 0 ]]; then
    echo "🎉 הכל תקין מצד Telegram. עכשיו אפשר לעבור לdeploy ל-Hetzner."
    echo "   תפתח את צ'אט הCowork ותכתוב: 'Telegram עובד, בוא נמשיך לHetzner'"
else
    echo "⚠️  תקן את הבעיות שסומנו ב-❌ קודם, ורק אז נמשיך לdeploy."
    echo "   אם משהו לא ברור — שלח לי בצ'אט את הoutput של הסקריפט."
fi
separator
