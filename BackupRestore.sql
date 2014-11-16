--(c) LGPL

use	master
go
if	object_id ( 'dbo.BackupRestore' , 'p' )	is	null
	exec	( 'create	proc	dbo.BackupRestore	as	select	ObjectNotCreated=	1/0' )
go
alter	proc	dbo.BackupRestore	-- бекап и восстановление баз данных
	@sAction		varchar ( 32 )
					-- команда BACKUP DATABASE/BACKUP LOG/restore
	,@sDBList		varchar ( 8000 )	output
					-- список через ";" баз данных(включая linked сервер перед именем базы через ".") до 100 значений (=MAXRECURSION default)
					-- output=список необработанных баз как индикатор ошибки
					-- (		restore	)null=пытаться восстанавливать все базы текущего сервера, при этом в output идёт null в качестве списка невостановленных баз
	,@sBackupDir		varchar ( 260 )
					-- каталог для файлов *.bak,*.trn доступный по тому же пути на сервере, где расположена каждая база данных; туда кладутся и оттуда берутся файлы бекапаов; для restore не должен содержать файлов, залоченных/недописанных или не предназначенных для восстановления; имя файла в пределах одной базы должно быть отсортировано по ASC
	,@bIsCoupled		bit
					-- ( backup	restore	)backup=именовать базы групповым признаком;restore=базы обрабатывать вместе, если одной не хватает, то не выполняться; разницей в начале снятия бекапа пренебрегаем
	,@sSQLDir		varchar ( 256 )
					-- (		restore	)каталог с файлами(mdf/ndf/ldf) баз данных MSSQL
	,@sOwner		varchar ( 256 )=	null
					-- (		restore	)владелец базы задаётся если база восстанавливается под logshipping без readonly
	,@sDestinationDir	varchar ( 260 )
					-- ( backup		)в этот каталог файлы бекапа должны быть перемещены после завершения команды, т.е. эти файлы уже не залочены и их можно распространять; каталог должен быть на том же диске, чтобы при перекладывании в него файл сразу стал доступным, если каталог на другом диске или сервере, то это длительная операция
	,@bIsCompressed		bit
					-- ( backup		)использовать сжатие sqlserver, хуже сжимает, чем архиватор; сильное сжатие нужно для передачи по медленной сети, когда затраты на сжатие окупаются ускорением передачи
	,@bIsCopyOnly		bit
					-- ( backup		)полный бекап, не нарушающий цепочку backup log
as
set	nocount	on
declare	@iError			int
	,@iRowCount		int
	,@sMessage		varchar ( 256 )
	,@bDebug		bit	-- 1=включить отладочные сообщения

	,@sBackupFile		varchar ( 256 )
	,@sBackupDirFileQuoted	varchar ( 256 )

	,@iBackupType		int
	,@sDBStandby		varchar ( 256 )
	,@sDBLive		varchar ( 256 )
	,@sCutoff		varchar ( 256 )

	,@dFirstLSN		numeric ( 25 , 0 )
	,@databaseBackupLSN	numeric ( 25 , 0 )

	,@sScript		varchar ( max )			-- не совместимо с sql 2000
	,@sScriptTemp		varchar ( max )

	,@c			cursor
	,@cDatabases		cursor
	,@cServers		cursor

	,@bRestoring		bit
	,@bStopWaiting		bit
	,@bQuorum		bit
	,@bAfterChange		bit
	,@bBeforeChange		bit

	,@sBackupSign		varchar ( 256 )
	,@sPattern		varchar ( 256 )
	,@sPatternFull		varchar ( 256 )
	,@sExtBak		varchar ( 256 )
	,@sExtTrn		varchar ( 256 )
	,@sDBLogShippedSign	varchar ( 256 )
	,@sBackupInfo		varchar ( 128 )
	,@sBackupInfoStr	varchar ( 128 )
	,@sDBListIn		varchar ( 8000 )
	,@sDBListOut		varchar ( 8000 )
	,@sDBListOut2		varchar ( 8000 )
	,@sServer		sysname
	,@sServerQuoted		nvarchar ( 256 )
	,@bServerAbsent		bit
	,@sExecAtServer		nvarchar ( 256 )
	,@sLockName		nvarchar ( 255 )

	,@sProjectSign		varchar ( 32 )
	,@sPostixUnique		nvarchar ( 256 )
	,@sProcName		sysname

	,@iDBCount		int		-- число баз в backup, восстанавливаемых синхронно
	,@dtMoment		datetime
	,@sDBDelimeter		varchar ( 2 )
----------
select	@bDebug=		1
	,@sExtBak=		'bak'							-- должны быть совместимы с MainenancePlan
	,@sExtTrn=		'trn'
	,@sProjectSign=		'363B1BEF8FA34873824C29D1EBC10C79'
	,@sDBLogShippedSign=	'z'+	@sProjectSign					-- считаем, что база под log shipping имеет уникальный префикс в имени
	,@sPostixUnique=	replace ( replace ( replace ( replace ( convert ( varchar ( 24 ) , getdate() , 121 ) , '-' , '' ) , ' ' , '' ) , ':' , '' ) , '.' , '' )
	,@sProcName=		'##'+	@sDBLogShippedSign+	'_PlanBR_'+	@sPostixUnique
	,@sDBDelimeter=		';'
	,@sDBListOut=		''
----------
if	@sBackupDir	not	like	'%\'	set	@sBackupDir=	@sBackupDir+	'\'
if	@sSQLDir	not	like	'%\'	set	@sSQLDir=	@sSQLDir+	'\'
----------
create	table	#BackupDir
(	ServerQuoted		nvarchar ( 128 )	NULL	default ( '' )
	,Subdirectory		nvarchar ( 128 )	NULL
	,depth			int
	,[file]			int )
----------
create	table	#BackupDirParsed
(	ServerQuoted		nvarchar ( 128 )	NULL
	,DB			varchar ( 128 )
	,FileName		nvarchar ( 128 )
	,Cutoff			varchar ( 256 )
	,Extension		varchar ( 256 )
	,IsRestoring		bit	)
----------
create	table	#DBStandbyLive
(	DBStandby		varchar ( 256 )
	,DBLive			varchar ( 256 )	)
----------
create	table	#files
(	DBStandby		varchar ( 256 )
	,DBLive			varchar ( 256 )
	,StandbyFileName	nvarchar ( 260 )
	,LiveFileName		nvarchar ( 260 )
	,TempFileName		nvarchar ( 260 ) )
----------
----------
create	table	#LabelOnly
(	MediaName		nvarchar ( 128 )
	,MediaSetId		uniqueidentifier
	,FamilyCount		int
	,FamilySequenceNumber	int
	,MediaFamilyId		uniqueidentifier
	,MediaSequenceNumber	int
	,MediaLabelPresent	tinyint
	,MediaDescription	nvarchar ( 255 )
	,SoftwareName		nvarchar ( 128 )
	,SoftwareVendorId	int
	,MediaDate		datetime )
----------
create	table	#BackupHeader	-- взято из процедуры master.dbo.sp_can_tlog_be_applied
(	BackupName		nvarchar ( 128 )	NULL
	,BackupDescription	nvarchar ( 256 )	NULL
	,BackupType		int
	,ExpirationDate		datetime		NULL
	,Compressed		int
	,Position		int
	,DeviceType		int
	,UserName		nvarchar ( 128 )	NULL
	,ServerName		nvarchar ( 128 )
	,databaseName		nvarchar ( 128 )
	,databaseVersion	int
	,databaseCreationDate	datetime
	,BackupSize		numeric ( 20 , 0 )	NULL
	,FirstLsn		numeric ( 25 , 0 )	NULL
	,LastLsn		numeric ( 25 , 0 )	NULL
	,CheckpointLsn		numeric ( 25 , 0 )	NULL
	,databaseBackupLsn	numeric ( 25 , 0 )	NULL
	,BackupStartDate	datetime
	,BackupFinishDate	datetime
	,SortOrder		int
	,CodePage		int
	,UnicodeLocaleId	int
	,UnicodeComparisonStyle	int
	,CompatibilityLevel	int
	,SoftwareVendorId	int
	,SoftwareVersionMajor	int
	,SoftwareVersionMinor	int
	,SoftwareVersionBuild	int
	,MachineName		nvarchar ( 128 )
	,Flags			int			NULL
	,BindingId		uniqueidentifier	NULL
	,RecoveryForkId		uniqueidentifier	NULL
	,Collation		nvarchar ( 128 )	null
	,FamilyGUID		uniqueidentifier	null
	,HasBulkLoggedData	bit			null
	,IsSnapshot		bit			null
	,IsReadOnly		bit			null
	,IsSingleUser		bit			null
	,HasBackupChecksums	bit			null
	,IsDamaged		bit			null
	,BeginsLogChain		bit			null
	,HasIncompleteMetadata	bit			null
	,IsForceOffline		bit			null
	,IsCopyOnly		bit			null
	,FirstRecoveryForkID	uniqueidentifier	null
	,ForkPointLSN		numeric ( 25 , 0 )	null
	,RecoveryModel		nvarchar ( 60 )		null
	,DifferentialBaseLSN	numeric ( 25 , 0 )	null
	,DifferentialBaseGUID	uniqueidentifier	null
	,BackupTypeDescription	nvarchar ( 60 )		null
	,BackupSetGUID		uniqueidentifier	null )
----------
create	table	#FileListOnly
(	LogicalName		nvarchar ( 128 ) 
	,PhysicalName		nvarchar ( 260 ) 
	,Type			char ( 1 ) 
	,FileGroupName		nvarchar ( 128 ) 
	,Size			numeric ( 20,0 ) 
	,MaxSize		numeric ( 20,0 ) 
	,FileID			bigint
	,CreateLSN		numeric ( 25,0 ) 
	,DropLSN		numeric ( 25,0 ) 	NULL
	,UniqueID		uniqueidentifier
	,ReadOnlyLSN		numeric ( 25,0 ) 	NULL
	,ReadWriteLSN		numeric ( 25,0 ) 	NULL
	,BackupSizeInBytes	bigint
	,SourceBlockSize	int
	,FileGroupID		int
	,LogGroupGUID		uniqueidentifier	NULL
	,DifferentialBaseLSN	numeric ( 25,0 ) 	NULL
	,DifferentialBaseGUID	uniqueidentifier
	,IsReadOnly		bit
	,IsPresent		bit )
----------
set	@sBackupSign=		'_backup_'
----------
if		convert ( varchar ( 10 ) , serverproperty ( 'ProductVersion' ) )	like	'%.%'	-- проверяем версию сервера
	and	10<	left ( convert ( varchar ( 10 ) , serverproperty ( 'ProductVersion' ) ) , charindex ( '.' , convert ( varchar ( 256 ) , serverproperty ( 'ProductVersion' ) ) ) - 1 )
begin
	alter	table	#LabelOnly	add
		MirrorCount		int	-- в документации поле называется Mirror_Count
		,IsCompressed		bit
----------
	alter	table	#BackupHeader	add
		CompressedBackupSize	bigint
		,containment		tinyint
----------
	alter	table	#FileListOnly	add
		TDEThumbprint		varbinary ( 32 )
----------
	set	@sPattern=		'%2[0-9][0-9][0-9][_][0-1][0-9][_][0-3][0-9][_][0-2][0-9][0-5][0-9][0-5][0-9][_][0-9][0-9][0-9][0-9][0-9][0-9][0-9].%'	-- маска имени backup файла из MaintenancePlan sql 2012
end
else
begin
	alter	table	#LabelOnly	add
		Mirror_Count		int
----------
	set	@sPattern=		'%2[0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9].%'								-- маска имени backup файла из MaintenancePlan sql 2005
end
----------
set	@sPatternFull=		'%[_]backup[_]'+	right ( @sPattern , len ( @sPattern )-	1 )							-- убираем первый %
----------
----------
create	table	#DBs
(	Sequence	smallint
	,ServerQuoted	varchar ( 256 )		-- linked server
	,Server		varchar ( 128 )
	,DBQuoted	varchar ( 256 )
	,DB		varchar ( 128 )
	,Value		varchar ( 256 ) )
----------
set	@sDBListIn=	@sDBList+	@sDBDelimeter	-- для упрощения проверки последнего значения в списке
----------
;with	cte	as
(	select	Pos=		charindex ( @sDBDelimeter , @sDBListIn )+	1
		,Value=		substring ( @sDBListIn , 1 , charindex ( @sDBDelimeter , @sDBListIn )-	1 )
		,Sequence=	1
	union	all
	select
		Pos=		charindex ( @sDBDelimeter , @sDBListIn , cte.pos )+	1
		,Value=		substring ( @sDBListIn , cte.Pos , charindex ( @sDBDelimeter , @sDBListIn , cte.Pos )-	cte.Pos )
		,Sequence=	cte.Sequence+	1
	from
		cte
	where
		cte.Pos<=	len ( @sDBListIn ) )
insert
	#DBs ( Sequence,	ServerQuoted,	Server,	DBQuoted,	DB,	Value )
select
	Sequence=	ROW_NUMBER()	over	( order	by	min ( Sequence ) )
	,quotename ( nullif ( parsename ( Value , 2 ), @@SERVERNAME ) )					-- исключаем собственное название сервера
	,nullif ( parsename ( Value , 2 ), @@SERVERNAME )						-- функцию используем только для упрощения обработки разделителя
	,quotename ( parsename ( Value , 1 ) )
	,parsename ( Value , 1 )
	,Value
from
	cte
where
		replace ( replace ( Value , ' ' , '' ) , char ( 13 )+	char ( 10 ) , '' )<>	''	-- пропускаем пустые, т.е. состоящие только из пробелов и/или CRLF
	and	(	DB_ID ( Value )		is	not	null					-- против injection
		or	parsename ( Value , 2 )	is	not	null )					-- на linked server существование базы не проверяем
group	by
	Value
union	all
select
	Sequence=	ROW_NUMBER()	over	( order	by	database_id )				-- не пересекается по условию с предыдущим select
	,null
	,null
	,quotename ( name , 1 )
	,name
	,name
from
	sys.databases
where
	@sDBListIn	is	null
----------
select	@iDBCount=	count (	* )	from	#DBs
----------
----------
if	@sAction=	'restore'
begin
	set	@cServers=	cursor	local	fast_forward	for
		select
			ServerQuoted
		from
			#DBs
		group	by
			ServerQuoted
		order	by
			ServerQuoted
----------
	set	@cDatabases=	cursor	local	fast_forward	for
		select
			b.DB
			,b.Cutoff
			,b.FileName
			,b.IsRestoring
			,IsAfterChange=	case	b.DB
						when	max ( bP.DB )	then	0
						else				1
					end
			,IsBeforeChange=case	b.DB
						when	min ( bN.DB )	then	0
						else				1
					end
		from
			#BackupDirParsed	b
			left	join	#BackupDirParsed	bN	on
				(	bN.ServerQuoted=	@sServerQuoted
				or	bN.ServerQuoted	is	null	and	@sServerQuoted	is	null )
			and	bN.DB=		b.DB
			and	b.Cutoff<	bN.Cutoff
			left	join	#BackupDirParsed	bP	on
				(	bP.ServerQuoted=	@sServerQuoted
				or	bP.ServerQuoted	is	null	and	@sServerQuoted	is	null )
			and	bP.DB=		b.DB
			and	bP.Cutoff<	b.Cutoff
		where
				b.ServerQuoted=	@sServerQuoted
			or	b.ServerQuoted	is	null	and	@sServerQuoted	is	null
		group	by
			b.DB
			,b.Cutoff
			,b.FileName
			,b.IsRestoring
		order	by
			b.DB
			,b.Cutoff						-- достаточно сортировать по части имени файла, т.к. там должно быть название базы и время бекапа
----------
	while	1=	1							-- цикл, т.к. считаем, что в @sBackupDir файлы могут приходить быстрее, чем успевает восстановить restore
	begin
		set	@sScript=	'
set	@sDBList=	null'
----------
		open	@cServers
----------
		truncate	table	#BackupDir
----------
		while	1=	1
		begin
			fetch	next	from	@cServers	into	@sServerQuoted
			if	@@fetch_status<>	0	break
----------
			set	@sExecAtServer=	isnull ( @sServerQuoted+	'...' , '' )+	'xp_dirtree'
----------
			insert	#BackupDir	( Subdirectory,	depth,	[file] )
			exec	@sExecAtServer
					@sBackupDir
					,0
					,1
			if	@@error<>	0
			begin
				set	@sMessage=	'Ошибка получения каталога файлов'
				goto	error
			end
----------
			update
				#BackupDir
			set
				ServerQuoted=	@sServerQuoted
			where
				ServerQuoted=	''			-- исключая null при отсутствии сервера
			if	@@error<>	0
			begin
				set	@sMessage=	'Ошибка получения каталога файлов'
				goto	error
			end
		end
----------
		close	@cServers
----------
		truncate	table	#BackupDirParsed
----------


select * from #BackupDir
select * from #DBs



		;with	cte	as
		(	select
				ServerQuoted
				,FileName=	Subdirectory
				,FileNameShort=	left ( Subdirectory , len ( Subdirectory ) - charindex ( '.' , reverse ( Subdirectory ) ) )
				,Extension=	right ( Subdirectory , charindex ( '.' , reverse ( Subdirectory ) )-	1 )
			from
				#BackupDir
			where
					[file]=	1
				and	depth=	1
				and	right ( Subdirectory , charindex ( '.' , reverse ( Subdirectory ) )-	1 )	in	( @sExtBak , @sExtTrn ) )
		insert
			#BackupDirParsed	( ServerQuoted,	DB,	FileName,	Cutoff,	Extension,	IsRestoring )	-- должно идти после заполнения #BackupDir
		select
			c1.ServerQuoted
			,d.DB
			,c1.FileName
			,replace ( c1.FileName , d.DB , '' )
			,c1.Extension
			,case
				when	c1.Extension=	@sExtTrn	and	c2.Extension=	@sExtBak	then	0
				else											1
			end
		from
			cte	c1
			left	join	cte	c2	on
				c2.FileNameShort=	c1.FileNameShort
			and	c2.Extension=		@sExtBak		-- если за одну дату будет два файла, то .trn нам не нужен, т.к. он идёт раньше .bak
			inner	join	#DBs	d	on
				c1.FileNameShort	like	'%'+	d.DB+	'%'
			and	(	d.ServerQuoted=	c1.ServerQuoted
				or	d.ServerQuoted	is	null	and	c1.ServerQuoted	is	null )
		select	@iError=	@@Error
			,@iRowCount=	@@RowCount
----------
		if	@iError<>	0	goto	error
--------	--
		if	@iRowCount=	0	break
----------
		open	@cServers
----------
		while	1=	1
		begin
			fetch	next	from	@cServers	into	@sServerQuoted
			if	@@fetch_status<>	0	break
----------
			set	@sScript=	@sScript+	'
----------
declare	@iFSO		int
	,@iError	int
----------
EXEC	sp_OACreate	''Scripting.FileSystemObject'',	@iFSO	OUT'
----------
			open	@cDatabases	-- должно идти после заполнения #BackupDirParsed
----------
			while	1=	1
			begin
				fetch	next	from	@cDatabases	into	@sDBStandby,	@sCutoff,	@sBackupFile,	@bRestoring,	@bAfterChange,	@bBeforeChange
				if	@@fetch_status<>	0
				begin
					set	@bStopWaiting=	1
----------
					break
				end
----------
				if	@bIsCoupled=	1
				begin
					if	( select
							count ( * )
						from
							#BackupDirParsed
						where
							Cutoff=	@sCutoff )	in	( @iDBCount/*bak*/,	@iDBCount*	2/*bak+trn*/ )
						set	@bQuorum=	1
					else
					begin
						if	@bQuorum=	0	set	@bStopWaiting=	1	else	set	@bQuorum=	0	-- если первый раз не хватает кол-ва бекапов для синхронного восстановления, то выходим и перечитываем, если во второй, то завершаем работу
----------
						break
					end
				end
----------
				set	@sBackupDirFileQuoted=	''''+	@sBackupDir+	@sBackupFile+	''''
----------
				set	@sScriptTemp=	'restore	labelonly	from	disk=	'+	@sBackupDirFileQuoted
----------
				if	@bDebug=	1
					print	( @sScriptTemp )
----------
				truncate	table	#LabelOnly					-- из-за наличия continue в тексте стирать лучше до
----------
				insert	#LabelOnly							-- лог jobа покажет ошибку на залоченном файле, который, например, ещё недоразжат архиватором
				exec	( @sScriptTemp )						-- датасет от restore нельзя подавать в insert
				if	@@error<>	0	continue				-- взяли не backup файл
----------
				set	@sScriptTemp=	'restore	headeronly	from	disk=	'+	@sBackupDirFileQuoted
----------
				if	@bDebug=	1
					print	( @sScriptTemp )
----------
				truncate	table	#BackupHeader
----------
				insert	#BackupHeader
				exec	( @sScriptTemp )						-- датасет от restore нельзя подавать в insert
				if	@@error<>	0	continue				-- битый заголовок?
----------
				select
					@iBackupType=		bh.BackupType
					,@sDBLive=		sd1.name
					--,@sDBStandby=		isnull ( sd2.name , sd1.name )
					,@dFirstLSN=		bh.FirstLSN
					,@databaseBackupLSN=	bh.databaseBackupLSN
				from
					#BackupHeader	bh
					inner	join	sys.databases	sd1	on
						sd1.name=	bh.databaseName
					left	join	sys.databases	sd2	on
						sd2.name=	@sDBLogShippedSign+	bh.databaseName
				where
						isnull ( sd2.name , sd1.name )=	@sDBStandby
				if	@@RowCount=	1						-- правильно распознали название базы в имени файла, например, 'DB' и 'DB1', здесь 1- часть названия базы или разделитель в имени файла?
				begin
					select	@sLockName=	upper ( @sDBStandby )+	@sProjectSign	-- сравнивается в бинарном виде, а одна и та же база может на разных серверах иметь имя в разном регистре
----------
					select
						@sScript=	@sScript+	'
----------
exec	@iError=	sp_getapplock						-- блокируем ресурс, если нужно запретить восстановление бекапа, то блокировку нужно запросить с @LockMode=Exclusive
				@Resource=	'''+	@sLockName+	'''
				,@LockMode=	''Shared''
				,@LockOwner=	''Session''
				,@LockTimeout=	0
if	@@Error<>	0	or	@iError<>	0
	set	@sDBList=	isnull ( @sDBList , '''' )+	'''+	@sDBStandby+	@sDBDelimeter+	''''
					where
						@bAfterChange=	1
----------
--@FirstLSNOld<=	@databaseBackupLSN
					set	@bRestoring=	@bRestoring&	case
											when	exists	( select
														1
													from
														#BackupDirParsed	d1
														,#BackupDirParsed	d2
													where
															d1.FileName=	@sBackupFile
														and	d2.Extension=	@sExtBak		-- если есть последующие .bak файлы этой базы, то промежуточные .trn и .bak восстанавливать не нужно
														and	left ( d2.FileName , patindex ( '%'+	d2.Cutoff+	'%',	d2.FileName )-	1 )=	@sDBLive+	@sBackupSign
														and	d1.Cutoff<	d2.Cutoff	 )	then	0
											else										1
										end
----------
					if	@bRestoring=	1
					begin
						set	@sScriptTemp=	'restore	filelistonly	from	disk=	'+	@sBackupDirFileQuoted+	'	with	nounload'
----------
						if	@bDebug=	1
							print	( @sScriptTemp )
----------
						truncate	table	#FileListOnly
----------
						insert	#FileListOnly
						exec	( @sScriptTemp )
						if	@@error<>	0	continue	-- битый список файлов?
----------
						if	@iBackupType=	1		-- Database
						begin
							set	@sScript=	@sScript+	'
----------
restore	database
	'+	@sDBStandby+	'
from
	disk=	'+	@sBackupDirFileQuoted+	'
with
	standby=	'''+	@sBackupDir+	@sDBStandby+	'.TUF'',
	nounload,
	replace,
	stats=	100'
----------
							select
								@sScript=	@sScript+	'
	,move	'''+	LogicalName+	'''	to	'''+	@sSQLDir+	case	@sDBLive
											when	@sDBStandby	then	''
											else				@sDBLogShippedSign
										end+	right ( PhysicalName , charindex ( '\' , reverse ( PhysicalName ) )-	1 )+	''''	-- вырезать имя файла из полного пути
							from
								#FileListOnly
							where
								@sSQLDir	is	not	null
						end
						else
							if	@iBackupType=	2	-- Transaction log
							begin
								/*if	not	exists	( select
												1
											from
												sys.master_files
											where
													redo_start_lsn=		@dFirstLSN
												and	db_name ( database_id )=@sDBStandby )	-- указанный backup не подходит для базы по порядку восстановления
								begin
									set	@sMessage=	'Порядок восстановления backup нарушен'
									goto	error
								end*/
----------
								set	@sScript=	@sScript+	'
----------
restore	log
	'+	@sDBStandby+	'
from
	disk=	'+	@sBackupDirFileQuoted+	'
with
	file=		1,
	standby=	'''+	@sBackupDir+	@sDBStandby+	'.TUF'''
							end
----------
						if		@sDBStandby<>	@sDBLive							-- далее работать с файлами нужно, если Standby и Live базы разные
							and	not	exists	( select	1	from	#DBStandbyLive	where	DBLive=	@sDBLive )
							insert	#DBStandbyLive	( DBStandby,	DBLive )					-- запоминаем восстанавливаемую базу
							values			( @sDBStandby,	@sDBLive )
					end
----------
					set	@sScript=	@sScript+	'
----------
'+	case	@bRestoring
		when	1	then	'if	@@Error=	0
	'
		else			''
	end+	'exec	sp_OAMethod	@iFSO,	''DeleteFile'',	null,	'+	@sBackupDirFileQuoted+	',	1'
----------
					truncate	table	#files
----------
					insert
						#files	( DBStandby,	DBLive,	StandbyFileName,	LiveFileName,	TempFileName )
					select
						DBstandby=		sb.name
						,DBLive=		li.name
						,StandbyFileName=	sbf.physical_name
						,LiveFileName=		lif.physical_name
						,TempFileName=		replace ( convert ( varchar ( 36 ) , newid() ) , '-' , '' )
					from
						#DBStandbyLive		d
						,sys.databases		sb
						,sys.master_files	sbf
						,sys.databases		li
						,sys.master_files	lif
					where
							sb.name=		d.DBStandby
						and	li.name=		d.DBLive
						and	sbf.database_id=	sb.database_id
						and	lif.database_id=	li.database_id
						and	sbf.file_id=		lif.file_id
----------
					select
						@sScript=	@sScript+	'
----------
alter	database	'+	DBStandby+	'	set	offline'
					from
						#DBStandbyLive
----------
					select
						@sScript=	@sScript+	'
----------
exec	sp_OAMethod
		@iFSO
		,''CopyFile''
		,'''+	isnull ( @sSQLDir+	right ( StandbyFileName , charindex ( '\' , reverse ( StandbyFileName ) )-	1 ) , StandbyFileName )+	'''
		,'''+	isnull ( @sSQLDir , left ( StandbyFileName , len ( StandbyFileName )-	charindex ( '\' , reverse ( StandbyFileName ) ) ) )+	TempFileName+	''''	--вырезать имя файла из полного пути и копируем файлы для live базы, но с другим именем, чтобы live базу не останавливать
					from
						#files
----------
					select
						@sScript=	@sScript+	'
----------
alter	database	'+	DBStandby+	'	set	online'
					from
						#DBStandbyLive
----------
					select
						@sScript=	@sScript+	'
----------
alter	database	'+	DBLive+		'	set	offline'
					from
						#DBStandbyLive
----------
					select
						@sScript=	@sScript+	'
----------
exec	sp_OAMethod	@iFSO,	''DeleteFile'',	'''+	@sSQLDir+	right ( LiveFileName , charindex ( '\' , reverse ( LiveFileName ) )-	1 )+	''''	--переименовываем скопированные под другим именем файлы в файлы Live базы
					from
						#files
----------
					select
						@sScript=	@sScript+	'
----------
exec	sp_OAMethod	@iFSO,	''MoveFile'',	'''+	@sSQLDir+	TempFileName+	''',	'''+	@sSQLDir+	right ( LiveFileName , charindex ( '\' , reverse ( LiveFileName ) )-	1 )+	''''	--переименовываем скопированные под другим именем файлы в файлы Live базы
					from
						#files
----------
					select
						@sScript=	@sScript+	'
----------
alter	database	'+	DBLive+		'	set	online'
					from
						#DBStandbyLive
----------
					select						-- базе в standby нельзя сделать sp_changedbowner
						@sScript=	@sScript+	'
----------
exec	'+	DBLive+	'..sp_changedbowner	'''+	@sOwner+	''''
					from
						#DBStandbyLive
				end
----------
				select
					@sScript=	@sScript+	'
----------
exec	@iError=	sp_releaseapplock		-- отпускаем блокировку
				@Resource=	'''+	@sLockName+	'''
				,@LockOwner=	''Session'''
				where
					@bBeforeChange=	1
			end
----------
			close	@cDatabases
----------
			set	@sScript=	@sScript+	'
----------
exec	sp_OADestroy	@iFSO
----------
error:'
			if	@sServerQuoted	is	not	null
				set	@sScript=	'
----------
exec	'+	@sServerQuoted+	'...sp_executesql
		@statement=	'''+	replace ( @sScript , '''' , '''''' )+	'''
		,@params=	N''@sDBList	varchar ( 8000 )	out''
		,@s=		@sDBList	out'
		end
----------
		close	@cServers
----------
		set	@sScript=	'create	proc	'+	@sProcName+	'
	@sDBList	varchar ( 8000 )	output
as
'+	@sScript
----------
		if	@bDebug=	1
		begin
			print	substring(@sScript,1,8000)
			print	substring(@sScript,8000,16000)
			print	substring(@sScript,16000,24000)
			print	substring(@sScript,24000,32000)
			print	substring(@sScript,32000,40000)
			print	substring(@sScript,40000,48000)
			print	substring(@sScript,48000,56000)
			print	substring(@sScript,56000,64000)
			print	substring(@sScript,64000,72000)
			print	substring(@sScript,72000,80000)
		end
----------
--		exec	( @sScript )
----------
		if	@bStopWaiting=	1	break
	end
----------
	deallocate	@cDatabases
end
----------------------------------------------------------------------------------------------------
else
	if		@sAction	like	'backup database'
		or	@sAction	like	'backup log'
	begin
		select	@sBackupInfo=		convert ( varchar ( 128 ) , convert ( varchar ( 16 ) , SERVERPROPERTY ( 'ProductVersion' ) )
					+	'|'+	convert ( varchar ( 8 ) , SERVERPROPERTY ( 'EngineEdition' ) )
					+	'|'+	@@servername+	isnull ( '.'+	db_name()+	'.'+	schema_name ( OBJECTPROPERTY ( @@procid,'OwnerId' ) )+	'.'+	object_name ( @@procid ) , '' )
					+	'|'+	convert ( varchar ( 2 ) , @@NESTLEVEL )
					+	isnull ( '|'+	app_name() , '' ) )
			,@sBackupInfoStr=	replace ( @sBackupInfo , '''' , '''''' )	-- чтобы при склеивании команды символ сохранился
----------
		select	@sScript=	'create	proc	'+	@sProcName+	'
	@sDBList	varchar ( 8000 )	output
as
set	@sDBList=	null'
			,@dtMoment=	getdate()
			,@sCutoff=	str ( year ( @dtMoment ) , 4 )
				+	'_'
				+	replace ( str ( month ( @dtMoment ) , 2 ) , ' ' , '0' )
				+	'_'
				+	replace ( str ( day ( @dtMoment ) , 2 ) , ' ' , '0' )
				+	'_'
				+	replace ( replace ( right ( convert ( varchar ( 23 ) , @dtMoment , 121 ) , 12 ) , ':' , '' ) , '.' , '_' )
				--+	'0000'
----------
		set	@c=	cursor	local	fast_forward	for
					select
						t.Server
						,t.ServerQuoted
						,IsAbsent=	case
									when		ss.isremote=	1
										or	(	t.Server	is	not	null
											and	ss.srvname	is		null )	then	1
									else									0
								end
					from
						#DBs	t
						left	join	sysservers	ss	on
							ss.srvname=	t.Server
						--and	ss.isremote=	0
					group	by
						t.Server
						,ss.srvname
						,ss.isremote
						,t.ServerQuoted
					order	by
						IsAbsent	desc
						,min ( t.Sequence )
----------
		open	@c
----------
		while	1=	1
		begin
			fetch	next	from	@c	into	@sServer,	@sServerQuoted,	@bServerAbsent
			if	@@fetch_status<>	0	break
----------
			if	@bServerAbsent=	1
			begin
				select
					@sDBListOut=	isnull ( @sDBListOut , '' )+	DB+	@sDBDelimeter
				from
					#DBs
				where
					Server=	@sServer
				group	by
					DB
				order	by
					min ( Sequence )
----------
				continue
			end
----------
			set	@sScriptTemp=	''
----------
			select
				@sScriptTemp=	@sScriptTemp+	'
----------
select							-- считаем, что нельзя одновременно бекапить несколько раз, хотя сервер и сам останавливается и ждёт завершения бекапа в другом соединении
	@sDBList=	isnull ( @sDBList , '''' )+	DB_NAME ( database_id )+	'''+	@sDBDelimeter+	'''
from
	master.sys.dm_exec_requests			-- sql 2005+
where
		command	like	''BACKUP%''
	and	DB_NAME ( database_id )=	'''+	DB+	'''
if	@@Rowcount=	0
begin
	BACKUP	'+	case
				when	@sAction	like	'%database'	then	'database'
				else							'log'
			end+	'
		'+	DBQuoted+	'
	TO
		DISK=	'''+	@sBackupDir+	DB+	@sBackupSign+	@sCutoff+	case	@bIsCoupled
												when	1	then	'0000'
												else			replace ( str ( Sequence , 4 ) , ' ' , '0' )
											end+	'.'+	case
														when	@sAction	like	'%database'	then	@sExtBak
														else							@sExtTrn
													end+	'''
	WITH
		'+	case	@bIsCompressed
				when	1	then	''
				else			'NO_'
			end+	'COMPRESSION'
		+	case	@bIsCopyOnly
				when	1	then	'
		,COPY_ONLY'
				else			''
			end+	'
		,FORMAT
		,INIT
		,NAME=	'''+	@sBackupInfoStr+	'''
		,SKIP
		,STATS=	100
	if	@@Error<>	0
		set	@sDBList=	isnull ( @sDBList , '''' )+	'''+	DB+	@sDBDelimeter+	'''
end'
			from
				#DBs
			where
					Server=	@sServer
				or	isnull ( Server , @sServer )	is	null
			order	by
				Sequence
----------
			select	@sScript=	@sScript+	case
									when	@sServer	is	null	then	@sScriptTemp
									else						'
----------
exec	'+	@sServerQuoted+	'...sp_executesql
		@statement=	'''+	replace ( @sScriptTemp , '''' , '''''' )+	'''
		,@params=	N''@sDBList	varchar ( 8000 )	out''
		,@s=		@sDBList	out'
								end
		end
----------
		deallocate	@c
		deallocate	@cServers
----------
		if	@bDebug=	1
		begin
			print	substring(@sScript,1,8000)
			print	substring(@sScript,8000,16000)
			print	substring(@sScript,16000,24000)
			print	substring(@sScript,24000,32000)
			print	substring(@sScript,32000,40000)
			print	substring(@sScript,40000,48000)
			print	substring(@sScript,48000,56000)
			print	substring(@sScript,56000,64000)
			print	substring(@sScript,64000,72000)
			print	substring(@sScript,72000,80000)
		end
----------
		exec	( @sScript )
----------
/*
		exec	@sProcName
				@sDBList=	@sDBListOut2	output
*/
----------
		set	@sDBListOut=	@sDBListOut+	isnull ( @sDBListOut2+	@sDBDelimeter , '' )
	end
----------
goto	done

error:

if	@sMessage	is	null	set	@sMessage=	'Ошибка ручного log shipping'
raiserror ( @sMessage , 18 , 1 )
--EXEC	@iError=	sp_OAGetErrorInfo	null,	@source	OUT,	@desc	OUT
--SELECT	OLEObject=	CONVERT ( binary ( 4 ),	@iError ),	source=	@source,	description=	@desc

done:

set	@sDBList=	@sDBListOut

----------
--if	cursor_status ( 'variable' , '@c' )<>	-2	deallocate	@c

drop	table
	#BackupDir