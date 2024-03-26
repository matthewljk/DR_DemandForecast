-- View: public.Net_Demand

-- DROP VIEW public."Net_Demand";

CREATE OR REPLACE VIEW public."Net_Demand"
 AS
 SELECT r."Date",
    r."Period",
    r."Total_Demand",
    r."TCQ" / 1000::double precision AS "TCQ",
    r."Total_Demand" - r."TCQ" / 1000::double precision AS "Net_Demand"
   FROM "TotalDemand_TCQ_Preview" r;

ALTER TABLE public."Net_Demand"
    OWNER TO sdcmktops;

