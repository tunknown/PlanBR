use	tempdb
go
if	object_id ( 'dbo.Entriggerate' , 'p' )	is	null
	exec	( 'create	proc	dbo.Entriggerate	as	return' )
go
alter	proc	dbo.Entriggerate
	@sTableName	nvarchar ( 512 )=	null	-- шаблон для like, для обработки всех таблиц задать %
as
set	nocount	on
----------
declare	@bDebug		bit=	1
	,@sObjectAlias	nvarchar ( 512 )
	,@sSign		varchar ( 32 )
	,@sExec		nvarchar ( max )
	,@sExec1	nvarchar ( max )
	,@sExec2	nvarchar ( max )
	,@sExec3	nvarchar ( max )
	,@iFirst	smallint
	,@iObjectId	integer
----------
set	@sSign=		'AAB74C7FFB7D4F0FB606CCF9F5293F5F'
----------
select
	*
	,ColumnNameSigned=	@sSign+	'_'+	ColumnName
into
	#ShowColumnDataTypes
from
	dbo.ShowColumnDataTypes
where
		ObjectAlias	like	@sTableName
	and	ObjectType=	'u'
order	by
	ObjectId			-- попытка сохранить недокументированную sumstr
	,ColumnId
----------
declare	c	cursor	local	fast_forward	for
	select
		ObjectId
		,ObjectAlias
	from
		#ShowColumnDataTypes
	group	by
		ObjectId
		,ObjectAlias
----------
open	c
----------
while	1=	1
begin
	fetch	next	from	c	into	@iObjectId,	@sObjectAlias
	if	@@fetch_status<>	0	break
----------
	if	exists	( select
				1
			from
				sysobjects	so
				,syscomments	sc
			where
					so.parent_obj=	@iObjectId
				and	so.xtype=	'tr'
				and	sc.id=		so.id
				and	text	like	'%'+	@sSign+	'%' )	-- предполагаем наличие сигнатуры в первых 4000 символах, чтобы она не попала на стык двух записей
		continue
----------
	set	@sExec=	'if	object_id ( '''+	@sObjectAlias+	'After'',	''tr'' )	is	not	null
	drop	trigger	'+	@sObjectAlias+	'After
'
----------
	exec	( @sExec )
----------
	set	@sExec=	'
create	trigger	'+	@sObjectAlias+	'After	on	'+	@sObjectAlias+	'
after	update,	delete
/*
(c) 2017 TUnknown
License:
public domain as executing code, cc0 as citation
*/
as
--сохраняем только исторические изменения
--в историю не попадают update ... set f2=f2, т.к. это не аудит
--в историю не попадают вычисляемые колонки
--MERGE внутри триггера поддерживать не нужно, будет несколько вызовов триггера
--следить за длиной названия полей при добавке уникализирующего суффикса
--выбирать все PK поля, при их отсутствии- поля из unique clustered или unique с наименьшим числом полей
--выбирать rowversion поле для сохранения последовательности операций insert/update/delete, если они произошли в течение 3 миллисекунд и datetime не даёт порядок следования
--сохранение изменений значений PK полей поддерживается только для случая update ... set f1=f1+N, при несоблюдении последовательности история может быть неверной
set	nocount	on
----------
declare	@x	xml
----------
;with	cte	as
(	select'
----------
	select	@iFirst=	1
		,@sExec1=	''
		,@sExec2=	''
----------
	select
		@sExec1=	@sExec1+	'
		'+	case	@iFirst
				when	1	then	''
				else			','
			end+	'd.'+	ColumnName
		,@sExec2=	@sExec2+	'
		,'+	ColumnNameSigned+	'=	i.'+	ColumnName
		,@iFirst=	@iFirst+	1
	from
		#ShowColumnDataTypes
	where
		ObjectId=	@iObjectId
	order	by
		ColumnId
----------
	select	@sExec=		@sExec
			+	@sExec1
			+	@sExec2
		,@sExec1=	''
----------
	select
		@sExec1=	@sExec1+	'
		,'+	@sSign+	'=	convert ( tinyint,	case
											when	i.'+	ColumnName+	'	is	null	then	1	-- удаление
											else					0
										end )'
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1
	order	by
		ColumnId
----------
	select	@sExec=		@sExec
			+	@sExec1
			+	'
	from
		deleted	d
		left	join	inserted	i	on'
		,@sExec1=	''
----------
	select
		@sExec1=	@sExec1+	'
			i.'+	ColumnName+	'=	d.'+	ColumnName+	'
	where
			not	update ( '+	ColumnName+	' )			-- список полей в PK/unique; при update PK join по нему не сработает'
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1
	order	by
		ColumnId
----------
	select	@sExec=		@sExec
			+	@sExec1
			+	'
	union	all
	select'
		,@iFirst=	1
		,@sExec1=	''
		,@sExec2=	''
----------
	select
		@sExec1=	@sExec1+	'
		'+	case	@iFirst
				when	1	then	''
				else			','
			end+	'd.'+	ColumnName
		,@sExec2=	@sExec2+	'
		,'+	ColumnNameSigned+	'=	i.'+	ColumnName
		,@iFirst=	@iFirst+	1
	from
		#ShowColumnDataTypes
	where
		ObjectId=	@iObjectId
	order	by
		ColumnId
----------
	select	@sExec=		@sExec
			+	@sExec1
			+	@sExec2
		,@sExec1=	''
		,@sExec2=	''
		,@sExec3=	''
		,@iFirst=	1
----------
	select
		@sExec1=	@sExec1+	'
		,'+	@sSign+	'=	convert ( tinyint,	case
											when	i.'+	ColumnName+	'	is	null	then	1	-- удаление
											else					0
										end )'
		,@sExec2=	@sExec2+	case
							when	1<	@iFirst	then	','
							else				''
						end+	'	'+	ColumnName
		,@sExec3=	@sExec3+	'		'+	case
										when	1<	@iFirst	then	'or'
										else				''
									end+	'	update ( '+	ColumnName+	' )'
		,@iFirst=	@iFirst+	1
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1
	order	by
		ColumnId
----------
	select	@sExec=		@sExec+	@sExec1+	'
	from
		( select
			*
			,'+	@sSign+	'=	row_number()	over	( order	by'+	@sExec2+	' )	-- если update PK не сохраняет последовательность записей, то join получится не с теми записями, поэтому, помечаем как ''p'', что говорит о возможной неточности логирования операции
		from
			deleted )	d
		left	join	( select
					*
					,'+	@sSign+	'=	row_number()	over	( order	by'+	@sExec2+	' )
				from
					inserted )	i	on
			i.'+	@sSign+	'=	d.'+	@sSign+	'	-- при update PK join по нему не сработает
	where								-- перечисление всех PK полей через OR
'+	@sExec3	+	'	)
select	@x=
	(select
		Tag
		,Parent
		,[d!1!xmlns:xsi]
		,[d!1!host]
		,[d!1!program]'
		,@sExec1=	''
----------
	select
		@sExec1=	@sExec1+	'
		,[r!2!'+	ColumnName+	']'
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1		-- ***здесь же обрабатывать и поля RowVersion
	order	by
		ColumnId
----------
	select	@sExec=		@sExec+	@sExec1+	'
		,[r!2!!hide]'
		,@sExec1=	''
----------
	select														-- хак- одинаковое имя элемента и атрибута, используется только одно, другое игнорируется
		@sExec1=	@sExec1
			+	'
		,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	ColumnId+	2 )+	'!!element]'
			+	case	IsNullable
					when	1	then	'
		,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	ColumnId+	2 )+	'!xsi:nil]'
					else			''
				end
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	0
	order	by
		ColumnId
----------
	select	@sExec=		@sExec+	@sExec1+	'
	from
		( select	distinct
			Tag=			1
			,Parent=		null
			,[d!1!xmlns:xsi]=	''http://www.w3.org/2001/XMLSchema-instance''	-- хак необходим из-за невозможности применения elementsxsinil, для поддержки xsi:nil
			,[d!1!host]=		host_name()
			,[d!1!program]=		program_name()'
		,@sExec1=	''
----------
	select
		@sExec1=	@sExec1+	'
			,[r!2!'+	ColumnName+	']=		convert ( '+	DataType+	',	null )'
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1		-- ***здесь же обрабатывать и поля RowVersion
	order	by
		ColumnId
----------
	select	@sExec=		@sExec+	@sExec1+	'
			,[r!2!!hide]=		convert ( smallint,		null )'		-- тип данных для 1024 полей
		,@sExec1=	''
----------
	select														-- хак- одинаковое имя элемента и атрибута, используется только одно, другое игнорируется
		@sExec1=	@sExec1
			+	'
			,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	ColumnId+	2 )+	'!!element]=		convert ( '+	DataType+	',	null )'
			+	case	IsNullable
					when	1	then	'
			,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	ColumnId+	2 )+	'!xsi:nil]=	convert ( varchar ( 4 ),	null )'
					else			''
				end
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	0
	order	by
		ColumnId
----------
	select	@sExec=		@sExec+	@sExec1+	'
		from
			cte
		where'
		,@sExec1=	'
				'+	@sSign+	'=	1'
----------
	select
		@sExec1=	@sExec1+	'
			or	(	'+	quotename ( ColumnName )+	'<>	'+	quotename ( ColumnNameSigned )+	'
				or	'+	quotename ( ColumnName )+	'	is	not	null	and	'+	quotename ( ColumnNameSigned )+	'	is		null
				or	'+	quotename ( ColumnName )+	'	is		null	and	'+	quotename ( ColumnNameSigned )+	'	is	not	null )'
	from
		#ShowColumnDataTypes
	where
		ObjectId=	@iObjectId
	order	by
		ColumnId




























----------
	select	@sExec=		@sExec+	@sExec1+	'
		union	all
		select
			Tag=			2
			,Parent=		1
			,[d!1!xmlns:xsi]=	null
			,[d!1!host]=		null
			,[d!1!program]=		null'
		,@sExec1=	''
----------
	select
		@sExec1=	@sExec1+	'
			,[r!2!'+	ColumnName+	']=		'+	quotename ( ColumnName )
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1		-- ***здесь же обрабатывать и поля RowVersion
	order	by
		ColumnId
----------
	select	@sExec=		@sExec+	@sExec1+	'
			,[r!2!!hide]=		1'
		,@sExec1=	''
----------
	select														-- хак- одинаковое имя элемента и атрибута, используется только одно, другое игнорируется
		@sExec1=	@sExec1
			+	'
			,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	ColumnId+	2 )+	'!!element]=		null'
			+	case	IsNullable
					when	1	then	'
			,['+	ColumnName+	'!'+	convert ( varchar ( 4 ),	ColumnId+	2 )+	'!xsi:nil]=	null'
					else			''
				end
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	0
	order	by
		ColumnId
----------
	select	@sExec=		@sExec+	@sExec1+	'
		from
			cte
		where'
		,@sExec1=	'
				'+	@sSign+	'=	1'
----------
	select
		@sExec1=	@sExec1+	'
			or	(	'+	quotename ( ColumnName )+	'<>	'+	quotename ( ColumnNameSigned )+	'
				or	'+	quotename ( ColumnName )+	'	is	not	null	and	'+	quotename ( ColumnNameSigned )+	'	is		null
				or	'+	quotename ( ColumnName )+	'	is		null	and	'+	quotename ( ColumnNameSigned )+	'	is	not	null )'

	from
		#ShowColumnDataTypes
	where
		ObjectId=	@iObjectId
	order	by
		ColumnId
----------
	select	@sExec=		@sExec+	@sExec1
		,@sExec2=	''
		,@iFirst=	1
----------
	select
		@sExec2=	@sExec2+	'
			'+	case
					when	1<	@iFirst	then	','
					else				''
				end
			+	quotename ( ColumnName )
		,@iFirst=	@iFirst+	1
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1		-- ***здесь же обрабатывать и поля RowVersion
	order	by
		ColumnId
----------
	select	@sExec=		@sExec+	'
		group	by'+	@sExec2
		,@sExec1=	''
		,@sExec2=	''
----------
	select
		@sExec2=	@sExec2+	'
			,[r!2!'+	ColumnName+	']=		'+	quotename ( ColumnName )+	'
--			,[r!2!q]=		null'
	from
		#ShowColumnDataTypes
	where
			ObjectId=	@iObjectId
		and	IsPrimaryKey=	1
	order	by
		ColumnId
----------
--***до и после обрабатываемого поля перечислить все предыдущие и последующие поля как=null

	select
		@sExec1=	@sExec1+	case
							when	c.IsFirstLast=	1	then	'
		union	all
		select
			Tag=			'+	convert ( varchar ( 4 ),	p.Sequence+	2 )+	'
			,Parent=		2
			,[d!1!xmlns:xsi]=	null
			,[d!1!host]=		null
			,[d!1!program]=		null'
					+	@sExec2
					+	'
			,[r!2!!hide]=		'+	convert ( varchar ( 4 ),	p.Sequence+	2 )
							else					''
						end
					+	case
							when		p.Sequence=	c.Sequence
								and	c.IsPrimaryKey=	0	then	'
			,['+	p.ColumnName+	'!'+	convert ( varchar ( 4 ),	p.Sequence+	2 )+	'!!element]=	case
							when	'+	quotename ( p.ColumnName )+	'=	'+	quotename ( p.ColumnNameSigned )+	'	then	null
							else								'+	quotename ( p.ColumnName )+	'
						end'+	case	c.IsNullable
								when	1	then	'
			,['+	p.ColumnName+	'!'+	convert ( varchar ( 4 ),	p.Sequence+	2 )+	'!xsi:nil]=	case
							when	'+	quotename ( p.ColumnName )+	'=	'+	quotename ( p.ColumnNameSigned )+	'	then	null
							when	'+	quotename ( p.ColumnName )+	'	is	null	and	'+	quotename ( p.ColumnNameSigned )+	'	is	null	then	null
							when	'+	quotename ( p.ColumnName )+	'	is	null				then	''true''
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
				(	UPDATE ( '+	quotename ( p.ColumnName )+	' )
				or	'+	@sSign+	'=	1
			and	(	'+	quotename ( p.ColumnName )+	'<>	'+	quotename ( p.ColumnNameSigned )+	' )
				or	'+	quotename ( p.ColumnName )+	'	is	not	null	and	'+	quotename ( p.ColumnNameSigned )+	'	is		null
				or	'+	quotename ( p.ColumnName )+	'	is		null	and	'+	quotename ( p.ColumnNameSigned )+	'	is	not	null )'
							else			''
						end
	from
		#ShowColumnDataTypes	p				-- цикл по каждому полю
		inner	join	#ShowColumnDataTypes	c	on
			c.ObjectId=	p.ObjectId
	where
			p.ObjectId=	@iObjectId
		and	p.IsPrimaryKey=	0
	order	by
		p.ColumnId
		,c.ColumnId
----------
	set	@sExec=	@sExec+	@sExec1+	' )	t
	order	by
		[r!2!f1]
		,[r!2!!hide]
	for
		xml	explicit )
----------
if	@x	is	not	null
	insert
		dbo.History	( DB,	Object,	Moment,	Login,	Data )
	select
		db_id()
		,@@procid	--select	id	from	sysobjects	where	parent_obj=	@@procid	-- для другой базы нужен динамический sql
		,getdate()
		,SYSTEM_USER
		,@x'
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
	end
--	exec	( @sExec )
end
deallocate	c
go
if	object_id ( 'dbo.Test',	'u' )	is	not	null
	drop	table	dbo.Test
create	table	dbo.Test
(	f1	int		primary	key
	,f2	varchar ( 256 )	null
	,f3	datetime	null
	,q	rowversion )
exec	dbo.Entriggerate
		@sTableName=	'dbo.Test'