import os

# Forzar bind público para Render incluso si el start command es "gunicorn app:app".
bind = f"0.0.0.0:{os.getenv('PORT', '10000')}"

# Config segura para instancia pequeña.
workers = int(os.getenv("WEB_CONCURRENCY", "1"))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "120"))

# Logs a stdout/stderr (Render los captura).
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")

