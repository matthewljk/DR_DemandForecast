SELECT [Date]
                ,DATEPART(YEAR, Date) AS 'Year'
                ,DATEPART(QUARTER, Date) AS 'Quarter'
                ,[Period]
                ,Demand+TCL+ISNULL(EnergyShortfall,0)+ISNULL(TransmissionLoss,0) AS 'Total_Demand'
                ,DATEPART(WEEKDAY, Date) % 7 AS 'Day_Type'
  FROM [NEMS].[dbo].[Real_Time_DPR]
  WHERE [Date] >= '2023-07-01'