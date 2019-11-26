DECLARE @YesterdayKey date =getdate()
DECLARE @MonthChange int = -(datepart(month, @YesterdayKey) - 1) % 3
DECLARE @DayChange int = -datepart(day, @YesterdayKey) + 1


SELECT Dateadd(month, @MonthChange, DateAdd(day, @DayChange, @YesterdayKey))


--DateTime QTDDateKey = YesterdayKey.AddDays(-YesterdayKey.Day + 1).AddMonths(-((int)((YesterdayKey.Month - 1) % 3))); 