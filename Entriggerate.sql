use	AAB74C7FFB7D4F0FB606CCF9F5293F5F	-- чтобы не забыть сменить БД
go
if	object_id ( 'dbo.Entriggerate' , 'p' )	is	null
	exec	( 'create	proc	dbo.Entriggerate	as	return' )
go
alter	proc	dbo.Entriggerate
	@sTableData	nvarchar ( 384 )	-- шаблон в формате database.schema.table с поддержкой like, для обработки всех таблиц задать %
	,@sTableLog	nvarchar ( 384 )	-- null/''=отключить триггер; имя таблицы лога в формате database.schema.table. Если база не указана, то в текущей базе
--(c) 2017-2020 TUnknown License: public domain/cc0
as
--сохраняем только исторические изменения
--в историю не попадают update ... set f2=f2, т.к. это не аудит
--в историю не попадают вычисляемые колонки
--MERGE внутри триггера поддерживать не нужно, будет несколько вызовов триггера
--следить за длиной названия полей при добавке уникализирующего суффикса
--выбирать все PK поля, при их отсутствии- поля из unique clustered или unique с наименьшим числом полей
--выбирать rowversion поле для сохранения последовательности операций insert/update/delete, если они произошли в течение 3 миллисекунд и datetime не даёт порядок следования
--сохранение изменений значений PK полей поддерживается только для случая update ... set f1=f1+N, при несоблюдении последовательности история может быть неверной
--после изменения полей таблицы триггер нужно пересоздавать
--errata: таблицы с unique, но без PK не поддерживаются
set	nocount	on
----------
declare	@bDebug			bit
	,@iRowCount		integer
	,@sTableName		sysname
	,@sTableSchema		sysname
	,@sTriggerName		nvarchar ( 512 )
	,@sTriggerName0		nvarchar ( 512 )
	,@sSign			varchar ( 32 )

	,@sExecSQLLog		nvarchar ( 256 )
	,@sExecSQLData		nvarchar ( 256 )
	,@sExecTriggerOrder	nvarchar ( 256 )

	,@sExec			nvarchar ( max )
	,@sExec01		nvarchar ( max )
	,@sExec02		nvarchar ( max )
	,@sExec03		nvarchar ( max )
	,@sExec04		nvarchar ( max )
	,@sExec05		nvarchar ( max )
	,@sExec06		nvarchar ( max )
	,@sExec07		nvarchar ( max )
	,@sExec08		nvarchar ( max )
	,@sExec09		nvarchar ( max )
	,@sExec10		nvarchar ( max )
	,@sExec11		nvarchar ( max )
	,@sExec12		nvarchar ( max )

	,@bFirst		bit
	,@iObjectId		integer
	,@sDBData		sysname
	,@sDBLog		sysname
	,@sSchemaData		sysname
	,@sSchemaLog		sysname
----------
create	table	#ShowColumnDataTypesData
(	ObjectId		int
	,Sequence		smallint
	,IsFirstLast		bit
	,ObjectName		sysname
	,SchemaName		sysname
	,ColumnName		sysname
	,ColumnNameQuoted	sysname
	,DataType		varchar ( 32 )
	,IsNullable		bit
	,IsPrimaryKey		bit

	,ColumnNameSigned	sysname	)
create	unique	clustered	index	IX001	on	#ShowColumnDataTypesData	( ObjectId,	Sequence )	-- без spool
----------
create	table	#ShowColumnDataTypesLog
(	ObjectId		int
	,Sequence		smallint
	,IsFirstLast		bit
	,ObjectName		sysname
	,SchemaName		sysname
	,ColumnName		sysname
	,ColumnNameQuoted	sysname
	,DataType		varchar ( 32 )
	,IsNullable		bit
	,IsPrimaryKey		bit

	,ColumnNameSigned	sysname	)
----------
select	@sSign=			'AAB74C7FFB7D4F0FB606CCF9F5293F5F'	-- предполагаем наличие сигнатуры в первых 4000 символах, чтобы она не попала на стык двух записей syscomments
	,@bDebug=		1
	,@sDBData=		isnull ( parsename ( @sTableData,	3 ),	db_name() )
	,@sDBLog=		isnull ( parsename ( @sTableLog,	3 ),	db_name() )
	,@sSchemaData=		isnull ( parsename ( @sTableData,	2 ),	'dbo'/*schema_name()*/ )
	,@sSchemaLog=		isnull ( parsename ( @sTableLog,	2 ),	'dbo'/*schema_name()*/ )
	,@sTableData=		parsename ( @sTableData,	1 )
	,@sTableLog=		parsename ( @sTableLog,		1 )

	,@sExecSQLLog=		@sDBLog+	'..sp_executesql'
	,@sExecSQLData=		@sDBData+	'..sp_executesql'
	,@sExecTriggerOrder=	@sDBData+	'..sp_settriggerorder'

	,@sExec01=		'
----------
insert
	#ShowColumnDataTypes/*0*/
select
	*
	,ColumnNameSigned=	'''+@sSign+	'_''+	ColumnName
from
	( select
		ObjectId=	o.Id,
		Sequence=	row_number()	over	( partition	by	o.Id	order	by	c.colid ),	--\гарантирует последовательность
	--не работает в случае только одного поля в таблице
		IsFirstLast=	case	
					when	row_number()	over	( partition	by	o.Id	order	by	c.colid )=	1	then	1
					when	row_number()	over	( partition	by	o.Id	order	by	c.colid	desc )=	1	then	0
				end,											-- else null
		ObjectName=	o.name,
		SchemaName=	schema_name ( o.uid ),
		ColumnName=	c.name,
		ColumnNameQuoted=	quotename ( c.name ),
		DataType=	convert ( nvarchar ( 256 ),	case
									when		t2.name	like	''%char''
										or	t2.name	like	''%binary''		then	t2.name
																+	'' ( ''
																+	case	c.prec
																		when	-1	then	''max''
																		else			convert ( varchar ( 256 ),	c.prec )
																	end+	'' )''
									when	t2.name	in	( ''numeric'',	''decimal'' )	then	t2.name
																+	'' ( ''
																+	convert ( varchar ( 256 ),	c.prec )
																+	'' , ''
																+	convert ( varchar ( 256 ),	c.scale )
																+	'' )''
									else								isnull ( t2.name,	t1.name )
								end ),
		IsNullable=	c.isnullable,
		IsPrimaryKey=	convert ( tinyint,	case	c.name		-- тип bit нежелательно использовать в агрегатах?
								when	INDEX_COL ( schema_name ( o.uid )+	''.''+	o.name,	ik.indid,	ik.keyno )	then	1
								else													0
							end )
	from
		sysobjects	o						-- через type_name ( typeproperty ( name , ''systemtype'' ) ) медленнее
		inner	join	syscolumns	c	on
			c.id=		o.id
		inner	join	systypes	t1	on			-- сработает ли inner для select Col_With_UserType into #temp?
			t1.xusertype=	c.xusertype
		left	join	systypes	t2	on			-- left для поддержки hierarchyid чензу=240
			t2.xtype=	t1.xtype
		and	t2.xtype=	t2.xusertype
		left	join	( select
					so.parent_obj
					,i.id
					,i.indid
				from
					sysobjects	so
					,sysindexes	i
				where
						so.xtype=	''pk''		-- дополнительный join из-за определения, что это primary
					and	i.name=		so.name
					and	i.id=		so.parent_obj )	opk	on
			opk.parent_obj=	o.id
		left	join	sysindexkeys	ik	on
			ik.id=		opk.id
		and	ik.indid=	opk.indid
		and	ik.colid=	c.colid
	where
			OBJECTPROPERTY ( o.id , ''IsMSShipped'' )=	0
		and	o.xtype=	''u''
		and	t2.name	not	in	( ''text'',	''ntext'',	''image'' ) )	t	-- inserted/deleted не поддерживают типы данных
where
		/*1*/
order	by
	ObjectId			-- попытка сохранить недокументированную sumstr
	,Sequence'
----------
set	@sExec=	replace ( replace ( @sExec01,	'/*0*/',	'Data' ),	'/*1*/',	'ObjectName	like	'''+	@sTableData+	'''	and	SchemaName	like	'''+	@sSchemaData+	'''' )
if	@bDebug=	1	print	@sExec
exec	@sExecSQLData
		@stmt=	@sExec
----------
set	@sExec=	replace ( replace ( @sExec01,	'/*0*/',	'Log' ),	'/*1*/',	'ObjectName	in	( '''+	@sTableLog+	''',	'''+	@sTableLog+	@sSign+	''' )	and	SchemaName=	'''+	@sSchemaLog+	'''' )
if	@bDebug=	1	print	@sExec
exec	@sExecSQLLog
		@stmt=	@sExec
----------
select
	@iRowCount=	count ( * )	-- если число полей совпадает, то эта таблица подходит; пересчитывать в зависимости от полей таблицы лога
from
	#ShowColumnDataTypesLog
where
		ObjectName=	@sTableLog
	and	SchemaName=	@sSchemaLog
----------
if	@iRowCount	not	in	( 0,	7 )
begin
	raiserror ( 'Под именем для таблицы логгирования есть другая таблица, задайте другое',	18,	1 )
	return
end
----------
if		@iRowCount=	0
	and	isnull ( @sTableLog,	'' )<>	''
begin
	set	@sExec01=	'
create	table	'+	@sSchemaLog+	'.'+	@sTableLog+	'
(	Sequence	bigint	unique	clustered	identity ( 1,	1 )
	,Moment		datetime		not null	default	getdate()
	,Host		sysname			null		default	host_name()
	,Login		sysname			not null	default	SYSTEM_USER
	,Object		nvarchar ( 384 )	null		default	db_name()+	''.''+	schema_name ( OBJECTPROPERTY ( @@procid,	''OwnerId'' ) )+	''.''+	object_name ( @@procid )	-- не идентификаторы, т.к. базу могут стереть, а лог оставить
	,Application	sysname			null		default	program_name()
	,Data		xml			null	)'	-- null, чтобы не запрещать update таблиц при сбое триггера
----------
	if	@bDebug=	1	print	@sExec01
	exec	@sExecSQLLog
			@stmt=	@sExec01		-- можно сделать xml->FILESTREAM
end
----------
declare	c	cursor	local	fast_forward	for
	select
		ObjectId
		,SchemaName
		,ObjectName
	from
		#ShowColumnDataTypesData
	group	by
		ObjectId
		,SchemaName
		,ObjectName
----------
open	c
----------
while	1=	1
begin
	fetch	next	from	c	into	@iObjectId,	@sTableSchema,	@sTableName
	if	@@fetch_status<>	0	break
----------
	select	@sTriggerName=	@sTableSchema+	'.'+	@sTableName+	'_AfterUpdateDelete_'+	@sSign
		,@sTriggerName0=@sDBData+	'.'+	@sTriggerName
----------
	if	object_id ( @sTriggerName0,	'tr' )	is	not	null
	begin
		set	@sExec=	'drop	trigger	'+	@sTriggerName	--***лучше это делать уже после получения текста триггера, если не получится, то не удалять существующий
----------
		exec	@sExecSQLData
				@stmt=	@sExec
	end
----------
	if	isnull ( @sTableLog,	'' )=	''			-- после drop	trigger
		continue
----------
	set	@sExec=	'
create	trigger	'+	@sTriggerName+	'	on	'+	@sTableSchema+	'.'+	@sTableName+	'
after	update,	delete
as
set	nocount	on
----------
if	object_id ( '''+	@sDBLog+	'.'+	@sSchemaLog+	'.'+	@sTableLog+	''',	''u'' )	is	null
begin
	raiserror ( ''
####################################################################################################
Логгирование не действует, т.к. таблица '+	@sDBLog+	'.'+	@sSchemaLog+	'.'+	@sTableLog+	' не найдена
####################################################################################################'',	0,	0 )
	return		-- логгирование не должно нарушать работу
end
----------
declare	@x	xml
----------
;with	cte	as
(	select'
----------
	select	@bFirst=	1
		,@sExec01=	''
		,@sExec02=	''
		,@sExec11=	''
		,@sExec12=	''
----------
	select
		@sExec01=	@sExec01+	'
		'+	case	@bFirst
				when	1	then	''
				else			','
			end+	'd.'+	ColumnName
		,@sExec02=	@sExec02+	'
		,'+	ColumnNameSigned+	'=	i.'+	ColumnName
		,@bFirst=	0

		,@sExec11=	@sExec11+	'
			,'+	ColumnName
		,@sExec12=	@sExec12+	'
					,'+	ColumnName
	from
		#ShowColumnDataTypesData
	where
		ObjectId=	@iObjectId
	order	by
		Sequence
----------
	select	@sExec=		@sExec
			+	@sExec01
			+	@sExec02
----------
	select
		@sExec=		@sExec+	'
		,'+	@sSign+	'=	convert ( tinyint,	case
											when	i.'+	ColumnName+	'	is	null	then	1	-- удаление
											else					0
										end )'
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1
	order	by
		Sequence
----------
	select	@sExec=		@sExec+	'
	from
		deleted	d
		left	join	inserted	i	on'
----------
	select
		@sExec=		@sExec+	'
			i.'+	ColumnName+	'=	d.'+	ColumnName+	'
	where
			not	update ( '+	ColumnName+	' )			-- список полей в PK/unique; при update PK join по нему не сработает'
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1
	order	by
		Sequence
----------
	select	@sExec=		@sExec+	'
	union	all
	select'
		,@bFirst=	1
		,@sExec03=	''
		,@sExec04=	''
----------
	select
		@sExec03=	@sExec03+	'
		'+	case	@bFirst
				when	1	then	''
				else			','
			end+	'd.'+	ColumnName
		,@sExec04=	@sExec04+	'
		,'+	ColumnNameSigned+	'=	i.'+	ColumnName
		,@bFirst=	0
	from
		#ShowColumnDataTypesData
	where
		ObjectId=	@iObjectId
	order	by
		Sequence
----------
	select	@sExec=		@sExec
			+	@sExec03
			+	@sExec04
		,@sExec05=	''
		,@sExec06=	''
		,@sExec07=	''
		,@bFirst=	1
----------
	select
		@sExec05=	@sExec05+	'
		,'+	@sSign+	'=	convert ( tinyint,	case
											when	i.'+	ColumnName+	'	is	null	then	1	-- удаление
											else					0
										end )'
		,@sExec06=	@sExec06+	case	@bFirst
							when	0	then	','
							else			''
						end+	'	'+	ColumnName
		,@sExec07=	@sExec07+	'		'+	case	@bFirst
										when	0	then	'or'
										else			''
									end+	'	update ( '+	ColumnName+	' )'
		,@bFirst=	0
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1
	order	by
		Sequence
----------
	select	@sExec=		@sExec
			+	@sExec05
			+	'
	from
		( select
			'+	@sSign+	'=	row_number()	over	( order	by'+	@sExec06+	' )	-- если update PK не сохраняет последовательность записей, то join получится не с теми записями, поэтому, помечаем как ''p'', что говорит о возможной неточности логирования операции'
			+	@sExec11+	'
		from
			deleted )	d
		left	join	( select
					'+	@sSign+	'=	row_number()	over	( order	by'+	@sExec06+	' )'
			+	@sExec12+	'
				from
					inserted )	i	on
			i.'+	@sSign+	'=	d.'+	@sSign+	'	-- при update PK join по нему не сработает
	where								-- перечисление всех PK полей через OR
'+	@sExec07	+	'	)
select	@x=
	(select
		Tag
		,Parent
		,[d!1!xmlns:xsi]
		,'
		,@sExec08=	''
----------
	select
		@sExec08=	@sExec08+		case	@sExec08
								when	''	then	''
								else			'
		,'
							end+	'[r!2!'+	ColumnName+	']'
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1		-- ***здесь же обрабатывать и поля RowVersion
	order	by
		Sequence
----------
	select	@sExec=		@sExec+	@sExec08+	'
		,[r!2!!hide]'
----------
	select														-- хак- одинаковое имя элемента и атрибута, используется только одно, другое игнорируется
		@sExec=		@sExec
			+	'
		,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	Sequence+	2 )+	'!!element]'
			+	case	IsNullable
					when	1	then	'
		,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	Sequence+	2 )+	'!xsi:nil]'
					else			''
				end
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	0
	order	by
		Sequence
----------
	select	@sExec=		@sExec+	+	'
	from
		( select
			Tag=			1
			,Parent=		null
			,[d!1!xmlns:xsi]=	''http://www.w3.org/2001/XMLSchema-instance'''	-- хак необходим из-за невозможности применения elementsxsinil, для поддержки xsi:nil
----------
	select
		@sExec=		@sExec+	'
			,[r!2!'+	ColumnName+	']=		convert ( '+	DataType+	',	null )'
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1		-- ***здесь же обрабатывать и поля RowVersion
	order	by
		Sequence
----------
	select	@sExec=		@sExec+	+	'
			,[r!2!!hide]=		convert ( smallint,		null )'		-- тип данных для 1024 полей
----------
	select														-- хак- одинаковое имя элемента и атрибута, используется только одно, другое игнорируется
		@sExec=		@sExec
			+	'
			,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	Sequence+	2 )+	'!!element]=		convert ( '+	DataType+	',	null )'
			+	case	IsNullable
					when	1	then	'
			,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	Sequence+	2 )+	'!xsi:nil]=	convert ( varchar ( 4 ),	null )'
					else			''
				end
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	0
	order	by
		Sequence
----------
	select	@sExec=		@sExec+	+	'
		union	all
		select
			Tag=			2
			,Parent=		1
			,[d!1!xmlns:xsi]=	null'
----------
	select
		@sExec=		@sExec+	'
			,[r!2!'+	ColumnName+	']=		'+	quotename ( ColumnName )
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1		-- ***здесь же обрабатывать и поля RowVersion
	order	by
		Sequence
----------
	select	@sExec=		@sExec+	+	'
			,[r!2!!hide]=		1'
		,@sExec09=	''
----------
	select														-- хак- одинаковое имя элемента и атрибута, используется только одно, другое игнорируется
		@sExec09=	@sExec09
			+	'
			,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	Sequence+	2 )+	'!!element]=		null'
			+	case	IsNullable
					when	1	then	'
			,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	Sequence+	2 )+	'!xsi:nil]=	null'
					else			''
				end
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	0
	order	by
		Sequence
----------
	select	@sExec=		@sExec+	@sExec09+	'
		from
			cte
		where'
		,@sExec09=	'
				'+	@sSign+	'=	1'
----------
	select
		@sExec09=	@sExec09+	'
			or	(	'+	ColumnNameQuoted+	'<>	'+	quotename ( ColumnNameSigned )+	'
				or	'+	ColumnNameQuoted+	'	is	not	null	and	'+	quotename ( ColumnNameSigned )+	'	is		null
				or	'+	ColumnNameQuoted+	'	is		null	and	'+	quotename ( ColumnNameSigned )+	'	is	not	null )'

	from
		#ShowColumnDataTypesData
	where
		ObjectId=	@iObjectId
	order	by
		Sequence
----------
	select	@sExec=		@sExec+	@sExec09+	'
		group	by'
		,@bFirst=	1
----------
	select
		@sExec=		@sExec+	'
			'+	case	@bFirst
					when	0	then	','
					else			''
				end
			+	ColumnNameQuoted
		,@bFirst=	0
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1		-- ***здесь же обрабатывать и поля RowVersion
	order	by
		Sequence
----------
	set	@sExec10=	''
----------
	select
		@sExec10=	@sExec10+	'
			,[r!2!'+	ColumnName+	']=		'+	ColumnNameQuoted
	from
		#ShowColumnDataTypesData
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1
	order	by
		Sequence
----------
--***до и после обрабатываемого поля перечислить все предыдущие и последующие поля как=null

	select
		@sExec=	@sExec+			case
							when	c.IsFirstLast=	1	then	'
		union	all
		select
			Tag=			'+	convert ( varchar ( 4 ),	p.Sequence+	2 )+	'
			,Parent=		2
			,[d!1!xmlns:xsi]=	null'
					+	@sExec10
					+	'
			,[r!2!!hide]=		'+	convert ( varchar ( 4 ),	p.Sequence+	2 )
							else					''
						end
					+	case
							when		p.Sequence=	c.Sequence
								and	c.IsPrimaryKey=	0	then	'
			,['+	p.ColumnName+	'!'+	convert ( varchar ( 4 ),	p.Sequence+	2 )+	'!!element]=	case
							when	'+	p.ColumnNameQuoted+	'=	'+	quotename ( p.ColumnNameSigned )+	'	then	null
							else								'+	p.ColumnNameQuoted+	'
						end'+	case	c.IsNullable
								when	1	then	'
			,['+	p.ColumnName+	'!'+	convert ( varchar ( 4 ),	p.Sequence+	2 )+	'!xsi:nil]=	case
							when	'+	p.ColumnNameQuoted+	'=	'+	quotename ( p.ColumnNameSigned )+	'	then	null
							when	'+	p.ColumnNameQuoted+	'	is	null	and	'+	quotename ( p.ColumnNameSigned )+	'	is	null	then	null
							when	'+	p.ColumnNameQuoted+	'	is	null				then	''true''
							else								null
						end'
								else			''
							end
							when		p.Sequence<>	c.Sequence
								and	c.IsPrimaryKey=	0	then	'
			,['+	c.ColumnName+	'!'+	convert ( varchar ( 4 ),	c.Sequence+	2 )+	'!!element]=	null'+	case	c.IsNullable
																		when	1	then	'
			,['+	c.ColumnName+	'!'+	convert ( varchar ( 4 ),	c.Sequence+	2 )+	'!xsi:nil]=	null'
																		else			''
																	end
							else						''
						end
					+	case	c.IsFirstLast
							when	0	then	'
		from
			cte
		where
				(	UPDATE ( '+	p.ColumnNameQuoted+	' )
				or	'+	@sSign+	'=	1
			and	(	'+	p.ColumnNameQuoted+	'<>	'+	quotename ( p.ColumnNameSigned )+	' )
				or	'+	p.ColumnNameQuoted+	'	is	not	null	and	'+	quotename ( p.ColumnNameSigned )+	'	is		null
				or	'+	p.ColumnNameQuoted+	'	is		null	and	'+	quotename ( p.ColumnNameSigned )+	'	is	not	null )'
							else			''
						end
	from
		#ShowColumnDataTypesData	p				-- цикл по каждому полю
		,#ShowColumnDataTypesData	c
	where
			p.ObjectId=	@iObjectId
		and	p.IsPrimaryKey=	0
		and	c.ObjectId=	@iObjectId
	order	by
		p.Sequence
		,c.Sequence
----------
	set	@sExec=	@sExec+	' )	t
	order	by
		'+	@sExec08+	'
	for
		xml	explicit )
----------
if	@x	is	not	null
	insert	'+	@sDBLog+	'.'+	@sSchemaLog+	'.'+	@sTableLog+	'	( Data )
	select	@x'
----------
	if	@bDebug=	1
	begin
		print	( substring ( @sExec,	1,	4000 ) )
		print	( substring ( @sExec,	4001,	8000 ) )
		print	( substring ( @sExec,	8001,	12000 ) )
		print	( substring ( @sExec,	12001,	20000 ) )
		print	( substring ( @sExec,	16001,	20000 ) )
		print	( substring ( @sExec,	20001,	24000 ) )
		print	( substring ( @sExec,	24001,	28000 ) )
		print	( substring ( @sExec,	28001,	32000 ) )
		print	( substring ( @sExec,	32001,	36000 ) )
		print	( substring ( @sExec,	36001,	40000 ) )
		print	( substring ( @sExec,	40001,	44000 ) )
		print	( substring ( @sExec,	44001,	48000 ) )
		print	( substring ( @sExec,	48001,	52000 ) )
		print	( substring ( @sExec,	52001,	56000 ) )
		print	( substring ( @sExec,	56001,	60000 ) )
		print	( substring ( @sExec,	60001,	64000 ) )
		print	( substring ( @sExec,	64001,	68000 ) )
		print	( substring ( @sExec,	68001,	72000 ) )
	end
----------
	exec	@sExecSQLData
			@stmt=	@sExec
----------
	select	@sExec01=		@sDBData+	'.'+	@sTableSchema+	'.'+	@sTableName
		,@sTriggerName0=	null
		,@sExec=		'
	select
		@sTriggerName0=	schema_name ( uid )+	''.''+	name
	from
		sysobjects
	where
			xtype=		''tr''
		and	parent_obj=	object_id ( @sExec01,	''u'' )
		and	objectproperty ( id,	''ExecIsFirstUpdateTrigger'' )=	1'
----------
	exec	@sExecSQLData
			@stmt=			@sExec
			,@params=		N'@sTriggerName0	nvarchar ( 256 )	output,	@sExec01	nvarchar ( 384 )'
			,@sTriggerName0=	@sTriggerName0	output
			,@sExec01=		@sExec01
----------
	if	@sTriggerName0	is	not	null
	begin
		exec	@sExecTriggerOrder
				@triggername=	@sTriggerName0
				,@order=	N'None'
				,@stmttype=	N'update'
	end
----------
	exec	@sExecTriggerOrder
			@triggername=	@sTriggerName
			,@order=	N'First'
			,@stmttype=	N'update'
----------
	select	@sTriggerName0=	null
		,@sExec=		'
	select
		@sTriggerName0=	schema_name ( uid )+	''.''+	name
	from
		sysobjects
	where
			xtype=		''tr''
		and	parent_obj=	object_id ( @sExec01,	''u'' )
		and	objectproperty ( id,	''ExecIsLastDeleteTrigger'' )=	1'
----------
	exec	@sExecSQLData
			@stmt=			@sExec
			,@params=		N'@sTriggerName0	nvarchar ( 256 )	output,	@sExec01	nvarchar ( 384 )'
			,@sTriggerName0=	@sTriggerName0	output
			,@sExec01=		@sExec01
----------
	if	@sTriggerName0	is	not	null
	begin
		exec	@sExecTriggerOrder
				@triggername=	@sTriggerName0
				,@order=	N'None'
				,@stmttype=	N'delete'
	end
----------
	exec	@sExecTriggerOrder
			@triggername=	@sTriggerName
			,@order=	N'Last'
			,@stmttype=	N'delete'
end
deallocate	c
go
----------------------------------------------------------------------------------------------------

--TEST

set	xact_abort	on

begin	tran

if	object_id ( 'dbo.Test',	'u' )	is	not	null
	drop	table	dbo.Test
create	table	dbo.Test
(	f1	int		primary	key
	,f2	varchar ( 256 )	null
	,f3	datetime	null
	,q	rowversion )
exec	dbo.Entriggerate
		@sTableData=	'dbo.Test'
		,@sTableLog=	'tempdb.dbo.Log'
insert	dbo.Test	( f1,	f2,	f3 )
select	1
	,'1'
	,getdate()
update	dbo.Test	set	f3=	getdate()

rollback