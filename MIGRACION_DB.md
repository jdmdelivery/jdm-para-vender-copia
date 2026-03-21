# Migración a PostgreSQL - JDM Cash Now

## Estado actual

**Implementado:**
- Modelos SQLAlchemy: `admins`, `usuarios`, `clientes`, `prestamos`, `pagos`, `banco`, `gastos`, `atrasos`, `pagos_admin`, `descuentos`, `cierres`, `auditoria`
- Capa PostgreSQL en **`credimapa_pg.py`** (un solo módulo en la raíz del repo para que Render/Git no dependan de la carpeta `appdb/`). Opcionalmente sigue existiendo el paquete `appdb/` en desarrollo local.
- Inicialización con `init_db.py`
- Integración parcial en `app.py`:
  - Login, current_user
  - get_bank_available, apply_cash_movement
  - log_action (auditoría)
  - loans_for_user, clients_for_user, payments_in_scope
  - compute_financial_kpis (n_cobradores)
  - api_notification_check
  - _revert_loan_financials

**Pendiente de migrar** (usar `store.` en memoria cuando no hay DATABASE_URL):
- Creación: new_client, new_loan, new_payment, register_admin, new_user
- Eliminación: delete_loan, delete_client, delete_payment, etc.
- Super admin: panel completo, admin_payments
- Banco: gastos, descuentos, depósitos, cierres
- Otras rutas que lean de `store.*`

## Cómo usar con PostgreSQL

1. **Configurar DATABASE_URL** (Render lo inyecta automáticamente):
   ```
   DATABASE_URL=postgresql://user:pass@host:5432/dbname
   ```

2. **Crear tablas y datos iniciales**:
   ```
   python init_db.py
   ```
   Con `--drop` para recrear desde cero:
   ```
   python init_db.py --drop
   ```

3. **Ejecutar la app**:
   ```
   gunicorn app:app
   ```

## Estructura multi-tenant

- `admins`: tenants (negocios). `id` = organization_id.
- `usuarios`: todos los logins. `admin_id` = tenant. `admin_id=NULL` = super_admin.
- Todas las tablas de negocio tienen `admin_id` para filtrar por tenant.
- El banco (`banco`) es un ledger: cada fila es un movimiento con `amount` (+/-).

## Lógica financiera (sin cambios)

- Préstamo creado: `banco` - monto_entregado
- Descuento inicial: `banco` + descuento
- Pago: `banco` + monto_pagado
- Gasto: `banco` - gasto
