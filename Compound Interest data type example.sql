/* An example of real v float rounding */

DECLARE @rate real = 0.05,
	@InterestAddedPerYear real = 12.0, -- monthly
	@InitialContibution real = 5000,
	@MonthlyContributions real = 100,
	@TimeYears real = 10

DECLARE 	@InterestPerPeriod real = (@rate / @InterestAddedPerYear),
	@InterestPeriods real = (@InterestAddedPerYear * @TimeYears)

SELECT
(POWER(1 + @InterestPerPeriod, @InterestPeriods) * @InitialContibution)
	, ((@MonthlyContributions * (POWER(1 + @InterestPerPeriod, @InterestPeriods) - 1) / @InterestPerPeriod)) -- * (1 + @InterestPerPeriod))

GO
DECLARE @rate float = 0.05,
	@InterestAddedPerYear float = 12.0, -- monthly
	@InitialContibution float = 5000,
	@MonthlyContributions float = 100,
	@TimeYears float = 10

DECLARE 	@InterestPerPeriod float = (@rate / @InterestAddedPerYear),
	@InterestPeriods float = (@InterestAddedPerYear * @TimeYears)

select (POWER(1 + @InterestPerPeriod, @InterestPeriods) * @InitialContibution)
	, ((@MonthlyContributions * (POWER(1 + @InterestPerPeriod, @InterestPeriods) - 1) / @InterestPerPeriod)) -- * (1 + @InterestPerPeriod))

GO
DECLARE @rate float = 0.05,
	@InterestAddedPerYear float = 12.0, -- monthly
	@InitialContibution float = 5000,
	@MonthlyContributions float = 100,
	@TimeYears float = 10

--P = A / ( 1 + r/n ) ^ (nt).
-- Initial = Target / POWER(1 + @InterestPerPeriod, @InterestPeriods)