-- ============================================================
-- Trigger para calcular flow_usd automáticamente
-- ============================================================
--
-- Este trigger calcula flow_usd = flow_btc * btc_price automáticamente
-- cuando se inserta/actualiza un precio BTC o un flow.
--
-- Uso:
--   psql $DATABASE_URL -f scripts/create_flow_usd_trigger.sql
--
-- ============================================================

-- Función que calcula flow_usd cuando se inserta/actualiza un precio BTC
CREATE OR REPLACE FUNCTION update_flow_usd_on_btc_price()
RETURNS TRIGGER AS $$
BEGIN
    -- Actualizar todos los flows de esta fecha que no tengan flow_usd
    UPDATE etf_flows
    SET flow_usd = flow_btc * NEW.price_usd,
        updated_at = NOW()
    WHERE date = NEW.date
      AND flow_btc IS NOT NULL
      AND (flow_usd IS NULL OR TG_OP = 'UPDATE');

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Función que calcula flow_usd cuando se inserta/actualiza un flow
CREATE OR REPLACE FUNCTION update_flow_usd_on_flow()
RETURNS TRIGGER AS $$
DECLARE
    btc_price NUMERIC;
BEGIN
    -- Solo calcular si tenemos flow_btc y no tenemos flow_usd
    IF NEW.flow_btc IS NOT NULL AND NEW.flow_usd IS NULL THEN
        -- Buscar el precio BTC para esta fecha
        SELECT price_usd INTO btc_price
        FROM btc_prices
        WHERE date = NEW.date;

        -- Si encontramos precio, calcular flow_usd
        IF btc_price IS NOT NULL THEN
            NEW.flow_usd := NEW.flow_btc * btc_price;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Eliminar triggers existentes si existen
DROP TRIGGER IF EXISTS trg_update_flow_usd_on_btc_price ON btc_prices;
DROP TRIGGER IF EXISTS trg_update_flow_usd_on_flow ON etf_flows;

-- Crear trigger en btc_prices (cuando se inserta/actualiza un precio)
CREATE TRIGGER trg_update_flow_usd_on_btc_price
    AFTER INSERT OR UPDATE OF price_usd ON btc_prices
    FOR EACH ROW
    EXECUTE FUNCTION update_flow_usd_on_btc_price();

-- Crear trigger en etf_flows (cuando se inserta/actualiza un flow)
CREATE TRIGGER trg_update_flow_usd_on_flow
    BEFORE INSERT OR UPDATE OF flow_btc ON etf_flows
    FOR EACH ROW
    EXECUTE FUNCTION update_flow_usd_on_flow();

-- ============================================================
-- Calcular flow_usd para registros existentes
-- ============================================================
UPDATE etf_flows f
SET flow_usd = f.flow_btc * bp.price_usd,
    updated_at = NOW()
FROM btc_prices bp
WHERE f.date = bp.date
  AND f.flow_btc IS NOT NULL
  AND f.flow_usd IS NULL;

-- Mostrar resultado
DO $$
DECLARE
    total_flows INTEGER;
    flows_with_usd INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_flows FROM etf_flows WHERE flow_btc IS NOT NULL;
    SELECT COUNT(*) INTO flows_with_usd FROM etf_flows WHERE flow_usd IS NOT NULL;

    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'TRIGGERS CREATED SUCCESSFULLY';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Total flows with flow_btc: %', total_flows;
    RAISE NOTICE 'Flows with flow_usd calculated: %', flows_with_usd;
    RAISE NOTICE '';
    RAISE NOTICE 'Triggers installed:';
    RAISE NOTICE '  - trg_update_flow_usd_on_btc_price (on btc_prices)';
    RAISE NOTICE '  - trg_update_flow_usd_on_flow (on etf_flows)';
    RAISE NOTICE '============================================================';
END $$;
