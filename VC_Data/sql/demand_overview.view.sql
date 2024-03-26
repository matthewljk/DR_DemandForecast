-- View: public.TotalDemand_TCQ_Preview

-- DROP VIEW public."TotalDemand_TCQ_Preview";

CREATE OR REPLACE VIEW public."TotalDemand_TCQ_Preview"
 AS
 SELECT r."Date",
    r."Period",
    r."Demand" + r."TCL" + COALESCE(r."Transmission_Loss", 0::real) AS "Total_Demand",
        CASE
            WHEN EXTRACT(dow FROM r."Date") > 0::numeric AND EXTRACT(dow FROM r."Date") < 6::numeric AND NOT (EXISTS ( SELECT 1
               FROM vesting_contract_ph vcph
              WHERE vcph."Date" = r."Date")) THEN v."TCQ_WD"
            ELSE v."TCQ_WE"
        END AS "TCQ"
   FROM "Real_Time_DPR" r
     LEFT JOIN ( SELECT vesting_contract_period."Year",
            vesting_contract_period."Quarter",
            vesting_contract_period."Period",
            vesting_contract_period."TCQ_WD",
            vesting_contract_period."TCQ_WE"
           FROM vesting_contract_period) v ON EXTRACT(year FROM r."Date") = v."Year"::numeric AND EXTRACT(quarter FROM r."Date") = v."Quarter"::numeric AND r."Period" = v."Period";

ALTER TABLE public."TotalDemand_TCQ_Preview"
    OWNER TO sdcmktops;

