-- Ejecutar una vez en PostgreSQL si la tabla cierres ya existía antes del cuadre semanal ampliado.
-- (La app también ejecuta estos ALTER vía ensure_cierres_schema().)

ALTER TABLE cierres ADD COLUMN IF NOT EXISTS fecha_inicio DATE;
ALTER TABLE cierres ADD COLUMN IF NOT EXISTS fecha_fin DATE;
ALTER TABLE cierres ADD COLUMN IF NOT EXISTS capital_cobrado NUMERIC(14,2);
ALTER TABLE cierres ADD COLUMN IF NOT EXISTS interes_cobrado NUMERIC(14,2);
ALTER TABLE cierres ADD COLUMN IF NOT EXISTS gastos_cuadre NUMERIC(14,2);
ALTER TABLE cierres ADD COLUMN IF NOT EXISTS descuentos_cuadre NUMERIC(14,2);
ALTER TABLE cierres ADD COLUMN IF NOT EXISTS ganancia_cuadre NUMERIC(14,2);
