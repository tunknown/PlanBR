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
	,@sBackupDir		varchar ( 260 )
					-- каталог для файлов *.bak,*.trn доступный по тому же пути на сервере, где расположена каждая база данных; туда кладутся и оттуда берутся файлы бекапаов; для restore не должен содержать файлов, залоченных/недописанных или не предназначенных для восстановления
	,@bIsCoupled		bit
					-- (backup/restore)backup=именовать базы групповым признаком;restore=базы обрабатывать вместе, если одной не хватает, то не выполняться; разницей в начале снятия бекапа пренебрегаем
	,@sSQLDir		varchar ( 256 )
					-- (restore)каталог с файлами(mdf/ndf/ldf) баз данных MSSQL
	,@sOwner		varchar ( 256 )=	null
					-- (restore)владелец базы задаётся если база восстанавливается под logshipping без readonly
	,@sDestinationDir	varchar ( 260 )
					-- (backup)в этот каталог файлы бекапа должны быть перемещены после завершения команды, т.е. эти файлы уже не залочены и их можно распространять; каталог должен быть на том же диске, чтобы при перекладывании в него файл сразу стал доступным, если каталог на другом диске или сервере, то это длительная операция
	,@bIsCompressed		bit
					-- (backup)использовать сжатие sqlserver, хуже сжимает, чем архиватор; сильное сжатие нужно для передачи по медленной сети, когда затраты на сжатие окупаются ускорением передачи
	,@bIsCopyOnly		bit
					-- (backup)полный бекап, не нарушающий цепочку backup log
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

	,@bRestoring		bit
	,@bStopWaiting		bit
	,@bQuorum		bit

	,@sBackupSign		varchar ( 256 )
	,@sPattern		varchar ( 256 )
	,@sPatternFull		varchar ( 256 )
	,@sExtBak		varchar ( 256 )
	,@sExtTrn		varchar ( 256 )
	,@sDBLogShippedSign	varchar ( 256 )
	,@sBackupInfo		varchar ( 128 )
	,@sBackupInfoStr	varchar ( 128 )
	,@sDBListIn		varchar ( 8000 )
	,@sServer		sysname
	,@sServerQuoted		nvarchar ( 256 )
	,@bServerNotExist	bit
	,@sExecAtServer		nvarchar ( 256 )

	,@sProjectSign		varchar ( 32 )
	,@sPostixUnique		nvarchar ( 256 )
	,@sProcName		sysname

	,@iDBCount		int		-- число баз в backup, восстанавливаемых синхронно
	,@dtMoment		datetime
----------
select	@bDebug=		1
	,@sExtBak=		'bak'							-- должны быть совместимы с MainenancePlan
	,@sExtTrn=		'trn'
	,@sProjectSign=		'363B1BEF8FA34873824C29D1EBC10C79'
	,@sDBLogShippedSign=	'z'+	@sProjectSign					-- считаем, что база под log shipping имеет уникальный префикс в имени
	,@sPostixUnique=	replace ( replace ( replace ( replace ( convert ( varchar ( 24 ) , getdate() , 121 ) , '-' , '' ) , ' ' , '' ) , ':' , '' ) , '.' , '' )
	,@sProcName=		'##'+	@sDBLogShippedSign+	'_PlanBR_'+	@sPostixUnique
----------
if	@sBackupDir	not	like	'%\'	set	@sBackupDir=	@sBackupDir+	'\'
if	@sSQLDir	not	like	'%\'	set	@sSQLDir=	@sSQLDir+	'\'
----------
create	table	#FolderBackup
(	Subdirectory		nvarchar ( 128 )	NULL
	,depth			int
	,[file]			int )
----------
create	table	#FolderBackupParsed
(	FileName		nvarchar ( 128 )
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
	,ValueQuoted	varchar ( 256 )
	,Value		varchar ( 128 ) )
----------
set	@sDBListIn=	@sDBList+	';'	-- для упрощения проверки последнего значения в списке
----------
;with	cte	as
(	select	Pos=		charindex ( ';' , @sDBListIn )+	1
		,Value=		substring ( @sDBListIn , 1 , charindex ( ';' , @sDBListIn )-	1 )
		,Sequence=	1
	union	all
	select
		Pos=		charindex ( ';' , @sDBListIn , cte.pos )+	1
		,Value=		substring ( @sDBListIn , cte.Pos , charindex ( ';' , @sDBListIn , cte.Pos )-	cte.Pos )
		,Sequence=	cte.Sequence+	1
	from
		cte
	where
		cte.Pos<=	len ( @sDBListIn ) )
insert
	#DBs ( Sequence,	ServerQuoted,	Server,	ValueQuoted,	Value )
select
	Sequence=	ROW_NUMBER()	over	( order	by	min ( Sequence ) )
	,quotename ( parsename ( Value , 2 ) )	-- функцию используем только для упрощения обработки разделителя
	,parsename ( Value , 2 )
	,quotename ( parsename ( Value , 1 ) )
	,parsename ( Value , 1 )
from
	cte
where
		replace ( replace ( Value , ' ' , '' ) , char ( 13 )+	char ( 10 ) , '' )<>	''	-- пропускаем пустые, т.е. состоящие только из пробелов и/или CRLF
	and	DB_ID ( Value )	is	not	null							-- против injection
group	by
	Value
----------
select	@iDBCount=	count (	* )	from	#DBs
----------
----------
if	@sAction=	'restore'
begin







	set	@c=	cursor	local	fast_forward	for
		select
			Cutoff
			,FileName
			,IsRestoring
		from
			#FolderBackupParsed
		order	by
			Cutoff
			,FileName	-- достаточно сортировать по имени файла, т.к. там должно быть название базы и время бекапа
----------
	while	1=	1
	begin
		truncate	table	#FolderBackup
----------
		insert	#FolderBackup	( Subdirectory,	depth,	[file] )
		exec	xp_dirtree	@sBackupDir,	0,	1
		if	@@error<>0
		begin
			set	@sMessage=	'Ошибка получения каталога файлов'
			goto	error
		end
----------
		truncate	table	#FolderBackupParsed
----------
		;with	cte	as
		(	select
				DB=		left ( Subdirectory , PATINDEX ( @sPatternFull , Subdirectory )-		1 )
				,Subdirectory
				,Cutoff=	substring ( Subdirectory , PATINDEX ( @sPattern , Subdirectory ) , len ( Subdirectory )-		PATINDEX ( @sPattern , Subdirectory )-	charindex ( '.' , reverse ( Subdirectory ) ) )	-- цифровая часть имени файла DBNAME_backup_201001211830.bak состоящая из даты+времени
				,Extension=	right ( Subdirectory , charindex ( '.' , reverse ( Subdirectory ) )-	1 )
			from
				#FolderBackup
			where
					[file]=	1
				and	depth=	1
				and	right ( Subdirectory , charindex ( '.' , reverse ( Subdirectory ) )-	1 )	in	( @sExtBak , @sExtTrn )
				and	Subdirectory	like	@sPatternFull )
		insert
			#FolderBackupParsed	( FileName,	Cutoff,	Extension,	IsRestoring )	-- должно идти после exec xp_dirtree
		select
			c1.Subdirectory
			,c1.Cutoff
			,c1.Extension
			,case
				when	c1.Extension=	@sExtTrn	and	c2.Extension=	@sExtBak	then	0
				else											1
			end
		from
			cte	c1
			left	join	cte	c2	on
				c2.Cutoff=	c1.Cutoff
			and	c2.DB=		c1.DB
			and	c2.Extension=	@sExtBak		-- если за одну дату будет два файла, то .trn нам не нужен, т.к. он идёт раньше .bak
			left	join	#DBs	d	on
				c1.DB=		d.Value
		where
				d.Value		is	not	null	-- берём только заданные бекапы
			or	@sDBListIn	is		null
		select	@iError=	@@Error
			,@iRowCount=	@@RowCount
----------
		if	@iError<>	0	goto	error
--------	--
		if	@iRowCount=	0	break
----------
		set	@sScript=	'


declare	@iError	int

exec	@iError=	sp_getapplock						-- блокируем ресурс, если нужно запретить восстановление бекапа, то блокировку нужно запросить с @LockMode=Exclusive
				@Resource=	'''+	@sProjectSign+	'''
				,@LockMode=	''Shared''
				,@LockOwner=	''Session''
				,@LockTimeout=	0

if	@iError<>	0
	goto	error

exec	@iError=	sp_releaseapplock
				@Resource=	'''+	@sProjectSign+	'''
				,@LockOwner=	''Session''



declare	@iFSO	int
EXEC	sp_OACreate	''Scripting.FileSystemObject'',	@iFSO	OUT'
----------
		open	@c	-- должно идти после заполнения #FolderBackupParsed
----------
		while	1=	1
		begin
			fetch	next	from	@c	into	@sCutoff,	@sBackupFile,	@bRestoring
			if	@@fetch_status<>	0
			begin
				set	@bStopWaiting=	1
----------
				break
			end
----------
			select	@sBackupDirFileQuoted=	''''+	@sBackupDir+	@sBackupFile+	''''
----------
			if	@bIsCoupled=	1
			begin
				if	( select
						count ( * )
					from
						#FolderBackupParsed
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
				,@sDBStandby=		isnull ( sd2.name , sd1.name )
				,@dFirstLSN=		bh.FirstLSN
				,@databaseBackupLSN=	bh.databaseBackupLSN
			from
				#BackupHeader	bh
				inner	join	sys.databases	sd1	on
					sd1.name=	bh.databaseName
				left	join	sys.databases	sd2	on
					sd2.name=	@sDBLogShippedSign+	bh.databaseName
----------
--@FirstLSNOld<=	@databaseBackupLSN
			set	@bRestoring=	@bRestoring&	case
									when	exists	( select
												1
											from
												#FolderBackupParsed	d1
												,#FolderBackupParsed	d2
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
				end
				else
					if	@iBackupType=	2	-- Transaction log
					begin
/*						if	not	exists	( select
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

'+	case	@bRestoring
		when	1	then	'if	@@Error=	0
	'
		else			''
	end	+	'exec	sp_OAMethod	@iFSO,	''DeleteFile'',	null,	'+	@sBackupDirFileQuoted+	',	1'
		end
----------
		close	@c
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

alter	database	'+	DBStandby+	'	set	offline'
		from
			#DBStandbyLive
----------
		select
			@sScript=	@sScript+	'

exec	sp_OAMethod	@iFSO,	''CopyFile'',	'''+	@sSQLDir+	right ( StandbyFileName , charindex ( '\' , reverse ( StandbyFileName ) )-	1 )+	''',	'''+	@sSQLDir+	TempFileName+	''''	--вырезать имя файла из полного пути и копируем файлы для live базы, но с другим именем, чтобы live базу не останавливать
		from
			#files
----------
		select
			@sScript=	@sScript+	'

alter	database	'+	DBStandby+	'	set	online'
		from
			#DBStandbyLive
----------
		select
			@sScript=	@sScript+	'

alter	database	'+	DBLive+		'	set	offline'
		from
			#DBStandbyLive
----------
		select
			@sScript=	@sScript+	'

exec	sp_OAMethod	@iFSO,	''DeleteFile'',	'''+	@sSQLDir+	right ( LiveFileName , charindex ( '\' , reverse ( LiveFileName ) )-	1 )+	''''	--переименовываем скопированные под другим именем файлы в файлы Live базы
		from
			#files
----------
		select
			@sScript=	@sScript+	'

exec	sp_OAMethod	@iFSO,	''MoveFile'',	'''+	@sSQLDir+	TempFileName+	''',	'''+	@sSQLDir+	right ( LiveFileName , charindex ( '\' , reverse ( LiveFileName ) )-	1 )+	''''	--переименовываем скопированные под другим именем файлы в файлы Live базы
		from
			#files
----------
		select
			@sScript=	@sScript+	'

alter	database	'+	DBLive+		'	set	online'
		from
			#DBStandbyLive
----------
		select						-- базе в standby нельзя сделать sp_changedbowner
			@sScript=	@sScript+	'

exec	'+	DBLive+	'..sp_changedbowner	'''+	@sOwner+	''''
		from
			#DBStandbyLive
----------
		set	@sScript=	@sScript+	'

exec	sp_OADestroy	@iFSO


error:

exec	@iError=	sp_releaseapplock		-- отпускаем блокировку
				@Resource=	'''+	@sProjectSign+	'''
				,@LockOwner=	''Session''
'
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
	deallocate	@c
end
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
	@sDBListOut	varchar ( 8000 )	output
as
set	@sDBListOut=	'''''
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
						,IsNotExist=	case
									when		t.Server	is	not	null
										and	ss.srvname	is		null	then	0
									else								1
								end
					from
						#DBs	t
						left	join	sysservers	ss	on
							ss.srvname=	t.Server
						--and	ss.isremote=	0
					group	by
						t.Server
						,t.ServerQuoted
					order	by
						min ( t.Sequence )
----------
		open	@c
----------
		while	1=	1
		begin
			fetch	next	from	@c	into	@sServer,	@sServerQuoted,	@bServerNotExist
			if	@@fetch_status<>	0	break
----------
			if	@bServerNotExist=	1
			begin
				set	@sDBListOut=	isnull ( @sDBListOut , '' )+	ValueQuoted+	';'
				continue
			end
----------
			select
				@sScriptTemp=	'
----------
select							-- считаем, что нельзя одновременно бекапить несколько раз, хотя сервер и сам останавливается и ждёт завершения бекапа в другом соединении
	@sDBListOut=	isnull ( @sDBListOut , '''' )+	DB_NAME ( database_id )+	'';''
from
	master.sys.dm_exec_requests			-- sql 2005+
where
		command	like	''BACKUP%''
	and	DB_NAME ( database_id )=	'''+	Value+	'''
if	@@Rowcount=	0
begin
	BACKUP	'+	case
				when	@sAction	like	'%database'	then	'database'
				else							'log'
			end+	'
		'+	ValueQuoted+	'
	TO
		DISK=	'''+		@sBackupDir+	Value+	@sBackupSign+	@sCutoff+	case	@bIsCoupled
													when	1	then	'0000'
													else			replace ( str ( Sequence , 4 ) , ' ' , '0' )
												end+	'.bak''
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
		set	@sDBListOut=	isnull ( @sDBListOut , '''' )+	'''+	Value+	';''
end'
				,@sExecAtServer=case
							when		@sServer=	@@ServerName
								or	@sServer	is	null	then	''
							else							@sServerQuoted+	'...'
						end+	'sp_executesql'
				,@sScript=	@sScript+	''+	case
										when		@sServer=	@@ServerName
											or	@sServer	is	null	then	@sScriptTemp
										else							'
----------
set	@sScript=	'+	@sScriptTemp+	'
exec	'+	@sExecAtServer+	'
		@statement=	@sScript
		,@params=	N''@sDBListOut	varchar ( 8000 )	out''
		,@s=		@sDBList	out'

									end
			from
				#DBs
			where
					Server=	@sServer
				or	isnull ( Server , @sServer )	is	null
			order	by
				Sequence
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
		end
----------
		deallocate	@c
	end
----------
goto	done

error:

if	@sMessage	is	null	set	@sMessage=	'Ошибка ручного log shipping'
raiserror ( @sMessage , 18 , 1 )
--EXEC	@iError=	sp_OAGetErrorInfo	null,	@source	OUT,	@desc	OUT
--SELECT	OLEObject=	CONVERT ( binary ( 4 ),	@iError ),	source=	@source,	description=	@desc

done:

----------
--if	cursor_status ( 'variable' , '@c' )<>	-2	deallocate	@c

drop	table
	#FolderBackup