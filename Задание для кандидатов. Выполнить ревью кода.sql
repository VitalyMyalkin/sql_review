create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int
-- Ключевые слова, названия системных функций и все операторы пишутся в нижнем регистре
as
set nocount on
begin
	-- По общему правилу, переменные объявляются один раз
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)
	-- Рекомендуется при объявлении типов не использовать длину поля "max"
	,@ErrorMessage varchar(8000)

-- Проверка на корректность загрузки
	if not exists (
		-- В условных операторах весь блок кода смещается на 1 отступ
		select 1
		/*
			При наименовании алиаса используются первые заглавные буквы каждого слова в названии объекта, 
			которому дают алиас. В случае, если алиас представляет собой системное слово, 
			добавляем первую согласную букву после заглавной из первого слова
		*/
		from syn.ImportFile as imf
		where imf.ID = @ID_Record
			and imf.FlagLoaded = cast(1 as bit)
	)
		begin
			set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'
			raiserror(@ErrorMessage, 3, 1)
			
			-- Пустая строка перед "return"
			return
		end

	-- Чтение из слоя временных данных
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	-- Пропущен оператор алиаса
	from syn.SA_CustomerSeasonal as cs
		-- Все виды join-ов указываются явно
		left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		left join dbo.Season as s on s.Name = cs.Season
		left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and c_dist.ID_mapping_DataSource = 1
		-- Сперва указывается поле присоединяемой таблицы
		left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	-- Определяем некорректные записи
	-- Добавляем причину, по которой запись считается некорректной
	select
		cs.*
		,case
			when c.ID is null
				-- При написании конструкции с "case", необходимо, чтобы "when" был под "case" с 1 отступом, "then" с 2 отступами
				then 'UID клиента отсутствует в справочнике "Клиент"'
			when c_dist.ID is null 
				then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null 
				then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null 
				then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			when try_cast(cs.DateBegin as date) is null 
				then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null 
				then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null 
				then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
		-- Все виды пересечений пишутся с 1 отступом
		left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor 
			-- Если есть "and", то он переносится на следующую строку и выравнивается на 1 табуляцию от "join" 
			and c_dist.ID_mapping_DataSource = 1
		left join dbo.Season as s on s.Name = cs.Season
		left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where c.ID is null
		or c_dist.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	-- Перед названием таблицы, в которую осуществляется  merge, into не указывается
	merge syn.CustomerSeasonal as cs
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	-- "then" записывается на одной строке с "when", независимо от наличия дополнительных условий
	when matched and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		update
		set 
			-- Перечисление всех атрибутов с новой строки
			ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		/*
		Для повышения читаемости кода длинные условия, формулы, выражения и т.п.,
		не умещающиеся на экране с разрешением 1366x768 должны быть разделены на несколько строк.
		Каждый параметр с новой строки. 
		*/
		insert (
			ID_dbo_Customer
			,ID_CustomerSystemType
			,ID_Season
			,DateBegin
			,DateEnd
			,ID_dbo_CustomerDistributor
			,FlagActive
		)
		values (
			s.ID_dbo_Customer
			,s.ID_CustomerSystemType
			,s.ID_Season
			,s.DateBegin
			,s.DateEnd
			,s.ID_dbo_CustomerDistributor
			,s.FlagActive
		);

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)
		raiserror(@ErrorMessage, 1, 1)

		-- Между "--" и текстом комментария должен быть один пробел
		-- Формирование таблицы для отчетности
		select top 100
			bir.Season as 'Сезон'
			,bir.UID_DS_Customer as 'UID Клиента'
			,bir.Customer as 'Клиент'
			,bir.CustomerSystemType as 'Тип клиента'
			,bir.UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,bir.CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(bir.DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateBegin) as 'Дата начала'
			-- Некорректная ссылка на поле из таблицы "BadInsertedRows"
			,isnull(format(try_cast(bir.DateEnd as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateEnd) as 'Дата окончания'
			,bir.FlagActive as 'Активность'
			,bir.Reason as 'Причина'
		from #BadInsertedRows as bir

		return
	end
end
