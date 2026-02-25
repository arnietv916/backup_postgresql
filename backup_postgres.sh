#!/usr/bin/env bash

set -o pipefail
umask 077

# ====== Настройки ======
BACKUP_DIR="${BACKUP_DIR:-/backup/pg}"
LOG_FILE="${LOG_FILE:-/var/log/db_backup.log}"

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-backup_user}"

export PGHOST PGPORT PGUSER

# Системные базы, которые обычно не бэкапят
EXCLUDE_DBS_REGEX='^(template0|template1|postgres)$'

# ====== Внутреннее ======
START_TS="$(date '+%Y-%m-%d %H:%M:%S')"
RUN_ID="$(date '+%Y%m%d_%H%M%S')_$$"
TMP_DIR="$(mktemp -d -t pg_backup_${RUN_ID}.XXXXXX)"

# Если /var/log недоступен — пишем в локальный файл
ensure_log_writable() {
  local dir
  dir="$(dirname "$LOG_FILE")"
  if ! mkdir -p "$dir" 2>/dev/null; then
    LOG_FILE="./db_backup.log"
  fi
  if ! touch "$LOG_FILE" 2>/dev/null; then
    LOG_FILE="./db_backup.log"
    touch "$LOG_FILE" 2>/dev/null || {
      echo "ERROR: cannot write log file anywhere" >&2
      exit 2
    }
  fi
}

log() {
  # log LEVEL MESSAGE...
  local level="$1"; shift
  local msg="$*"
  printf '%s [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$level" "$msg" >> "$LOG_FILE"
}

cleanup() {
  # Всегда чистим временные файлы
  if [[ -n "${TMP_DIR:-}" && -d "$TMP_DIR" ]]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

need_cmd() {
  command -v "$1" >/dev/null 2>&1
}

check_prereqs() {
  local missing=0
  for c in psql pg_dump gzip df mktemp; do
    if ! need_cmd "$c"; then
      log "ERROR" "Не найдено: $c (скрипт не может продолжать)"
      missing=1
    fi
  done
  [[ $missing -eq 0 ]]
}

bytes_free_kb() {
  # df -Pk PATH -> available KB
  df -Pk "$1" 2>/dev/null | awk 'NR==2 {print $4}'
}

ensure_dirs() {
  if ! mkdir -p "$BACKUP_DIR" 2>/dev/null; then
    log "ERROR" "Не могу создать/открыть каталог бэкапов: $BACKUP_DIR"
    return 1
  fi
  if [[ ! -w "$BACKUP_DIR" ]]; then
    log "ERROR" "Нет прав на запись в каталог бэкапов: $BACKUP_DIR"
    return 1
  fi
  return 0
}

list_databases() {
  # Список берём штатно через psql
  # -At: no headers, tuples only
  # datistemplate=false исключает шаблоны
  psql -At -d postgres -c "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;" 2>/dev/null
}

safe_name() {
  # заменим пробелы и странные символы
  echo "$1" | tr ' /' '__' | tr -cd '[:alnum:]_.-'
}

dump_db() {
  local db="$1"
  local dump_file="$2"
  # plain SQL
  pg_dump -d "$db" -F p --no-owner --no-privileges > "$dump_file" 2> "$TMP_DIR/pg_dump_${db}.err"
}

gzip_db() {
  local in_file="$1"
  local out_gz="$2"
  gzip -c "$in_file" > "$out_gz" 2> "$TMP_DIR/gzip.err"
}

test_gzip() {
  local gz="$1"
  gzip -t "$gz" >/dev/null 2>&1
}

move_atomic() {
  local src="$1"
  local final="$2"
  local tmp="$BACKUP_DIR/.tmp.$(basename "$final").$$"
  if mv -f "$src" "$tmp" 2>/dev/null && mv -f "$tmp" "$final" 2>/dev/null; then
    return 0
  fi
  # попытка убрать хвосты
  rm -f "$tmp" 2>/dev/null || true
  return 1
}

# ====== Main ======
ensure_log_writable
log "INFO" "Старт бэкапа PostgreSQL (run_id=$RUN_ID)"

if ! check_prereqs; then
  log "ERROR" "Остановка: не выполнены требования по утилитам"
  exit 2
fi

if ! ensure_dirs; then
  log "ERROR" "Остановка: каталог бэкапов недоступен"
  exit 2
fi

# Быстрая проверка места 
tmp_free_kb="$(bytes_free_kb "$TMP_DIR" || echo 0)"
bkp_free_kb="$(bytes_free_kb "$BACKUP_DIR" || echo 0)"
if [[ "${tmp_free_kb:-0}" -lt 102400 ]]; then # <100MB
  log "ERROR" "Слишком мало места для временных файлов (доступно ~${tmp_free_kb}KB). Остановка."
  exit 3
fi
if [[ "${bkp_free_kb:-0}" -lt 102400 ]]; then # <100MB
  log "ERROR" "Слишком мало места в $BACKUP_DIR (доступно ~${bkp_free_kb}KB). Остановка."
  exit 3
fi

# Получаем список баз
DB_LIST="$(list_databases)"
if [[ -z "$DB_LIST" ]]; then
  log "ERROR" "Не получил список баз. Проверь доступ/пароль (psql не смог подключиться)."
  exit 4
fi

log "INFO" "Найдено баз: $(echo "$DB_LIST" | wc -l | tr -d ' ')"

ok_count=0
fail_count=0

while IFS= read -r db; do
  [[ -z "$db" ]] && continue
  if [[ "$db" =~ $EXCLUDE_DBS_REGEX ]]; then
    log "INFO" "Пропуск системной базы: $db"
    continue
  fi

  db_safe="$(safe_name "$db")"
  stamp="$(date '+%Y%m%d_%H%M%S')"
  dump_file="$TMP_DIR/${db_safe}_${stamp}.sql"
  gz_file="$TMP_DIR/${db_safe}_${stamp}.sql.gz"
  final_file="$BACKUP_DIR/${db_safe}_${stamp}.sql.gz"

  log "INFO" "База: $db — начинаю дамп"
  if ! dump_db "$db" "$dump_file"; then
    err_tail="$(tail -n 2 "$TMP_DIR/pg_dump_${db}.err" 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g')"
    log "ERROR" "База: $db — дамп не создан. Причина: ${err_tail:-неизвестно}"
    rm -f "$dump_file" 2>/dev/null || true
    ((fail_count++))
    continue
  fi
  log "INFO" "База: $db — дамп создан"

  log "INFO" "База: $db — сжимаю gzip"
  if ! gzip_db "$dump_file" "$gz_file"; then
    log "ERROR" "База: $db — gzip не получился"
    rm -f "$dump_file" "$gz_file" 2>/dev/null || true
    ((fail_count++))
    continue
  fi
  # дамп больше не нужен
  rm -f "$dump_file" 2>/dev/null || true
  log "INFO" "База: $db — архив создан"

  log "INFO" "База: $db — проверяю целостность архива"
  if ! test_gzip "$gz_file"; then
    log "ERROR" "База: $db — архив битый (gzip -t не прошёл)"
    rm -f "$gz_file" 2>/dev/null || true
    ((fail_count++))
    continue
  fi
  log "INFO" "База: $db — архив исправен"

  # Проверим место перед переносом 
  bkp_free_kb="$(bytes_free_kb "$BACKUP_DIR" || echo 0)"
  if [[ "${bkp_free_kb:-0}" -lt 10240 ]]; then # <10MB
    log "ERROR" "База: $db — мало места в $BACKUP_DIR, перенос отменён"
    rm -f "$gz_file" 2>/dev/null || true
    ((fail_count++))
    continue
  fi

  log "INFO" "База: $db — переношу в $BACKUP_DIR"
  if ! move_atomic "$gz_file" "$final_file"; then
    log "ERROR" "База: $db — не смог перенести архив в $BACKUP_DIR"
    rm -f "$gz_file" 2>/dev/null || true
    ((fail_count++))
    continue
  fi

  log "INFO" "База: $db — готово ($final_file)"
  ((ok_count++))
done <<< "$DB_LIST"

log "INFO" "Финиш. Успешно: $ok_count, с ошибками: $fail_count"
log "INFO" "Завершение запуска (run_id=$RUN_ID, started=$START_TS)"

# Возврат кода:
# 0 — всё ок
# 1 — были ошибки по отдельным базам (но скрипт отработал корректно)
if [[ $fail_count -gt 0 ]]; then
  exit 1
fi
exit 0
