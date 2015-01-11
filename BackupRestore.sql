--(c) LGPL
use	master
go
if	object_id ( 'dbo.BackupRestore' , 'p' )	is	null
	exec	( 'create	proc	dbo.BackupRestore	as	select	ObjectNotCreated=	1/0' )
go
alter	proc	dbo.BackupRestore	-- бекап и восстановление баз данных
	@tPlan		text			-- maintenance план в XML
	,@tMessage	text=	null	output	-- список сообщений в XML, например, ошибок о то, что БД есть в плане, но она не обработалась

--выполнять по мере генерации или сгенерировать всё и после этого выполнять?

/*
	@sAction		varchar ( 32 )
					-- команда BACKUP DATABASE/BACKUP LOG/restore
	,@sDBList		varchar ( 8000 )	output
					-- список через ";" баз данных(включая linked сервер перед именем базы через ".") до 100 значений (=MAXRECURSION default)
					-- output=список необработанных баз как индикатор ошибки
					-- (		restore	)null=пытаться восстанавливать все базы текущего сервера, при этом в output идёт null в качестве списка невостановленных баз
					-- (		restore	)DB_1=DB, восстановить базу DB под именем DB_1, например, для тестового сервера с несколькими версиями баз
	,@sBackupDir		varchar ( 260 )
					-- каталог для файлов *.bak,*.trn доступный по тому же пути на сервере, где расположена каждая база данных; туда кладутся и оттуда берутся файлы бекапаов; для restore не должен содержать файлов, залоченных/недописанных или не предназначенных для восстановления; имя файла в пределах одной базы должно быть отсортировано по ASC

***bit->varchar(256) для backup(возможно, с макросами), для restore игнорировать содержимое, считать только за признак- задано или нет
***@bIsCoupled->Cutoff


***имя сервера в название базы
***восстановление базы под другим именем для тестового сервера
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
*/
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
	,@sScriptTemp		nvarchar ( max )

	,@cTemp			cursor
	,@cSuit			cursor
	,@cBodySteps		cursor

	,@bRestoring		bit
	,@bStopWaiting		bit
	,@bQuorum		bit
	,@bAfterDBChange	bit
	,@bBeforeDBChange	bit

	,@bHasFileworkOnRestore	bit

	,@sBackupSign		varchar ( 256 )
	,@sPattern		varchar ( 256 )
	,@sExtBak		varchar ( 256 )
	,@sExtTrn		varchar ( 256 )
	,@sExtension		varchar ( 256 )
	,@sDBLogShippedSign	varchar ( 256 )
	,@sBackupInfo		varchar ( 128 )
	,@sBackupInfoStr	varchar ( 128 )
	,@sDBListOut		varchar ( 8000 )
	,@sDBListOut2		varchar ( 8000 )
	,@bServerAbsent		bit
	,@sExecAtServer		nvarchar ( 256 )
	,@sLockName		nvarchar ( 255 )

	,@sProjectSign		varchar ( 32 )
	,@sPostixUnique		nvarchar ( 256 )
	,@sProcName		sysname

	,@iDBCount		int		-- число баз в backup, восстанавливаемых синхронно
	,@dtMoment		datetime
	,@sDBDelimeter		varchar ( 2 )

	,@sFileNameBak		varchar ( 256 )

	,@iXML			integer
	,@iParent		smallint

	,@sSQLDir		nvarchar ( 260 )
	,@sDBList		varchar ( 8000 )
	,@bIsCoupled		bit
	,@sOwner		sysname

	,@bIsAsyncronous	bit



	,@sSuit			sysname

	,@iStep			smallint
	,@sAction		varchar ( 256 )
	,@sFromServer		sysname
	,@sFromDB		sysname
	,@sFromFolder		sysname
	,@sFromFile		sysname
	,@sToServer		sysname
	,@sToDB			sysname
	,@sToFolder		sysname
	,@sToFile		sysname
	,@bIsCompressed		bit
	,@bIsCopyOnly		bit
	,@sServer		sysname
	,@sServerQuoted		sysname
	,@sDB			sysname
	,@sDBQuoted		sysname
	,@sAccess		varchar ( 256 )

	,@iSequenceMax		smallint
----------
select	@bDebug=		1
	,@sExtBak=		'bak'							-- должны быть совместимы с MainenancePlan
	,@sExtTrn=		'trn'
	,@sProjectSign=		'363B1BEF8FA34873824C29D1EBC10C79'
	,@sDBLogShippedSign=	'z'+	@sProjectSign					-- считаем, что база под log shipping имеет уникальный префикс в имени
	,@sDBDelimeter=		';'
	,@sDBListOut=		''
----------
create	table	#Suit	-- группа команд
-- поля группы спускаются в содержимое группы; если в содержимом заданы поля, отличные от групповых, то ошибка
(	Id		smallint	not null	unique	clustered	--backup/restore		автоматически присваивается в OPENXML, используется для сортировки
	,Suit		sysname		null		--backup/restore		-- ""=автоматический выбор группировки, для backup- дата+время, для restore- исключить из FileName имя базы и восстанавливать не все файлы по базе, а все файлы внутри suit; считаем набор баз неделимой пачкой, бекапить все вместе под одним Cutoff и восстанавливать все вместе с одним одним Cutoff, если не хватает файла, то не восстанавливать ни одного; сортировка всегда asc
	,IsAsyncronous	bit		null		--backup/restore
	,Server		sysname		null		--на этом linked сервере выполняется процедура этой группы
--далее поля для удобства массового заполнения в #Body
	,Action		varchar ( 256 )	null		-- backup database/backup log/restore
	,FromServer	sysname		null		--backup/restore
	,FromDB		sysname		null		--backup/restore
	,FromFolder	sysname		null		--restore
	,FromFile	sysname		null		--restore		-- заменить имя базы и сервера в имени файла на пустой символ и этот остаток использовать как CutOff
	,ToServer	sysname		null		--restore
	,ToDB		sysname		null		--restore
	,ToFolder	sysname		null		--backup/restore
	,ToFile		sysname		null		--backup
	,IsCompressed	bit		null		--backup
	,IsCopyOnly	bit		null		--backup
,check	( Action	in	( 'backup database' , 'backup log' , 'restore' ) )	)
----------
create	table	#Body	-- содержимое группы
(	Id		smallint	not null	unique	clustered	--backup/restore
	,Parent		smallint	not null	foreign	key	references	#Suit ( Id ) --backup/restore, как напоминание, всё равно- FK не работает

	,Action		varchar ( 256 )	null		-- backup database/backup log/restore; при backup процедура создаётся на FromServer, при restore процедура создаётся на ToServer, в пределах одного Suit это поле должно быть заполнено одинаково
	,FromServer	sysname		null		--backup/restore		для выборки из списка файлов, например, если база с одним именем существует на 2х серверах
	,FromDB		sysname		null		--backup/?restore
	,FromFolder	sysname		null		--restore
	,FromFile	sysname		null		--restore		-- заменить имя базы и сервера в имени файла на пустой символ и этот остаток использовать как CutOff
	,ToServer	sysname		null		--restore
	,ToDB		sysname		null		--restore
	,ToFolder	sysname		null		--backup/restore		-- для restore= перенос в этот фолдер после восстановления?
	,ToFile		sysname		null		--backup
	,IsCompressed	bit		null		--backup
	,IsCopyOnly	bit		null		--backup
	,Access		varchar ( 256 )	null		--restore		null= не создавать копию базы, read=создавать копию, базу оставлять в readonly, write=создавать копию, для базы разрешать запись
	,Message	varchar ( max )	null		-- текст ошибки при попытке обработки этой записи
	,Server		as	case
					when	Action	like	'backup%'	then	FromServer
					else						ToServer
				end
	,ServerQuoted	as	quotename (	case
							when	Action	like	'backup%'	then	FromServer
							else						ToServer
						end )
	,DB		as	case
					when	Action	like	'backup%'	then	FromDB
					else						ToDB
				end
	,DBQuoted	as	quotename (	case
							when	Action	like	'backup%'	then	FromDB
							else						ToDB
						end )
	,Folder		as	case
					when	Action	like	'backup%'	then	ToFolder
					else						FromFolder
				end
	,FileName	as	case
					when	Action	like	'backup%'	then	ToFile
					else						FromFile
				end



	,FileNameShort	as	left (	case
						when	Action	like	'backup%'	then	ToFile
						else						FromFile
					end , len (	case
								when	Action	like	'backup%'	then	ToFile
								else						FromFile
							end ) - charindex ( '.' , reverse (	case
													when	Action	like	'backup%'	then	ToFile
													else						FromFile
												end ) ) )
	,Extension	as	right (	case
						when	Action	like	'backup%'	then	ToFile
						else						FromFile
					end , charindex ( '.' , reverse (	case
											when	Action	like	'backup%'	then	ToFile
											else						FromFile
										end ) )-	1 )


,unique	( Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFile,	IsCompressed,	IsCopyOnly )
,check	( Action	in	( 'backup database' , 'backup log' , 'restore' ) )
,check	( Access	in	( 'read' , 'write' ) )
--,check	( FromDB	is	not	null	or	ToDB		is	not	null )	--/ограничение не работает при автозаполнении параметров; если обрабатывать все доступные базы, то параметр не указывается
--,check	( FromFolder	is	not	null	or	ToFolder	is	not	null )	--\
 )
----------
if	isnull ( datalength ( @tPlan ) , 0 )<	4
begin
	select	@sMessage=	'План резервирования в неверном формате',
		@iError=	-3
	goto	error
end
----------
EXEC	@iError=	sp_xml_preparedocument	@iXML	OUTPUT,	@tPlan		-- sql2000+
if	@@Error<>	0	or	@iError<>	0
begin
	select	@sMessage=	'Ошибка XML 1',
		@iError=	-3
	goto	error
end
----------
insert
	#Suit	( Id,	Suit,	IsAsyncronous,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFile,	IsCompressed,	IsCopyOnly )
SELECT
	Id
	,Suit
	,IsAsyncronous
	,Action
	,FromServer
	,FromDB
	,FromFolder
	,FromFile
	,ToServer
	,ToDB
	,ToFolder
	,ToFile
	,IsCompressed
	,IsCopyOnly
FROM
	OPENXML	( @iXML,	'/PlanBR/s',	1 )	-- атрибуты case sensitive
WITH
	( Id		smallint	'@mp:id'
	,Suit		sysname
	,IsAsyncronous	bit
	,Action		varchar ( 256 )
	,FromServer	sysname
	,FromDB		sysname
	,FromFolder	sysname
	,FromFile	sysname
	,ToServer	sysname
	,ToDB		sysname
	,ToFolder	sysname
	,ToFile		sysname
	,IsCompressed	bit
	,IsCopyOnly	bit )
----------
insert
	#Body	( Id,	Parent,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFile,	IsCompressed,	IsCopyOnly,	Access )
select
	Id
	,Parent
	,Action
	,FromServer
	,FromDB
	,FromFolder
	,FromFile
	,ToServer
	,ToDB
	,ToFolder
	,ToFile
	,IsCompressed
	,IsCopyOnly
	,Access
from
	OPENXML	( @iXML,	'/PlanBR/s/b',	1 )	-- атрибуты case sensitive
WITH
	( Id		smallint	'@mp:id'
	,Parent		smallint	'@mp:parentid'
	,Action		varchar ( 256 )
	,FromServer	sysname
	,FromDB		sysname
	,FromFolder	sysname
	,FromFile	sysname
	,ToServer	sysname
	,ToDB		sysname
	,ToFolder	sysname
	,ToFile		sysname
	,IsCompressed	bit
	,IsCopyOnly	bit
	,Access		varchar ( 256 ) )
----------
update
	b
set
	@sMessage=	isnull ( @sMessage , '' )
		+	case
				when	( b.Action<>	s.Action	or	isnull ( b.Action,	s.Action )	is	null )	and	@sMessage	not	like	'%,Action%'	then	',Action'
				when	b.FromServer<>	s.FromServer									and	@sMessage	not	like	'%,FromServer%'	then	',FromServer'
				when	b.FromDB<>	s.FromDB									and	@sMessage	not	like	'%,FromDB%'	then	',FromDB'
				when	b.FromFolder<>	s.FromFolder									and	@sMessage	not	like	'%,FromFolder%'	then	',FromFolder'
				when	b.FromFile<>	s.FromFile									and	@sMessage	not	like	'%,FromFile%'	then	',FromFile'
				when	b.ToServer<>	s.ToServer									and	@sMessage	not	like	'%,ToServer%'	then	',ToServer'
				when	b.ToDB<>	s.ToDB										and	@sMessage	not	like	'%,ToDB%'	then	',ToDB'
				when	b.ToFolder<>	s.ToFolder									and	@sMessage	not	like	'%,ToFolder%'	then	',ToFolder'
				when	b.ToFile<>	s.ToFile									and	@sMessage	not	like	'%,ToFile%'	then	',ToFile'
				when	b.IsCompressed<>s.IsCompressed									and	@sMessage	not	like	'%,IsCompressed%' then	',IsCompressed'
				when	b.IsCopyOnly<>	s.IsCopyOnly									and	@sMessage	not	like	'%,IsCopyOnly%'	then	',IsCopyOnly'
				else																					''
			end
	,Action=	isnull ( b.Action,	s.Action )
	,FromServer=	coalesce ( b.FromServer,s.FromServer/*,	case
									when	isnull ( b.Action,	s.Action )	like	'backup%'	then	@@servername
									else										null
								end*/ )
	,FromDB=	isnull ( b.FromDB,	s.FromDB )
	,FromFolder=	isnull ( b.FromFolder,	s.FromFolder )+	case
									when	isnull ( b.FromFolder,	s.FromFolder )	not	like	'%\'	then	'\'
									else										''
								end
	,FromFile=	isnull ( b.FromFile,	s.FromFile )
	,ToServer=	coalesce ( b.ToServer,	s.ToServer/*,	case
									when	isnull ( b.Action,	s.Action )	like	'restore'	then	@@servername
									else										null
								end*/ )
	,ToDB=		isnull ( b.ToDB,	s.ToDB )
	,ToFolder=	isnull ( b.ToFolder,	s.ToFolder )+	case
									when	isnull ( b.ToFolder,	s.ToFolder )	not	like	'%\'	then	'\'
									else										''
								end
	,ToFile=	isnull ( b.ToFile,	s.ToFile )
	,IsCompressed=	isnull ( b.IsCompressed,s.IsCompressed )
	,IsCopyOnly=	isnull ( b.IsCopyOnly,	s.IsCopyOnly )
from
	#Suit	s
	,#Body	b
where
	b.Parent=	s.Id
----------
set	@sMessage=	nullif ( stuff ( @sMessage , 1 , 1 , '' ) , '' )
----------
if	@sMessage	is	not	null
begin
	set	@sMessage=	'Ошибка подачи параметров в атрибутах: '+	@sMessage
	goto	error
end
----------
select
	Parent
	,Server
into
	#SuitServer
from
	#Body
group	by
	Parent
	,Server
----------
if	exists	( select
			1
		from
			#SuitServer
		group	by
			Parent
		having
			1<	count ( * ) )
begin
	set	@sMessage=	'Ошибка подачи параметров в атрибутах: задан разный сервер внутри одной группы'
	goto	error
end
----------
create	table	#DBss
(	Parent		smallint
	,Server		sysname
	,DB		sysname
	,DBQuoted	sysname	)
----------
create	table	#BackupDir
(	ServerQuoted		nvarchar ( 128 )	null		default ( '' )
	,Folder			nvarchar ( 260 )	not null	default ( '' )
	,FileName		nvarchar ( 255 )	null
	,Level			tinyint			null
	,IsFile			bit			null )
----------
create	table	#Server_DBFiles
(	dbid			smallint
	,fileid			smallint
	,DBName			sysname
	,LogicalName		nvarchar ( 260 )
	,filename		nvarchar ( 260 )
	,TempName		varchar ( 32 ) )
----------
create	table	#Generation_Files
(	DBStandby		varchar ( 256 )
	,DBLive			varchar ( 256 )
	,StandbyFileName	nvarchar ( 260 )
	,LiveFileName		nvarchar ( 260 )
	,TempFileName		nvarchar ( 260 )
	,IsDBStandbyCreate	bit

	,StandbyPath	as	left ( StandbyFileName , len ( StandbyFileName )-	charindex ( '\' , reverse ( StandbyFileName ) )+	1 )
	,StandbyFile	as	right ( StandbyFileName , charindex ( '\' , reverse ( StandbyFileName ) )-	1 )
	,LivePath	as	left ( LiveFileName , len ( LiveFileName )-	charindex ( '\' , reverse ( LiveFileName ) )+	1 )
	,LiveFile	as	right ( LiveFileName , charindex ( '\' , reverse ( LiveFileName ) )-	1 ) )
----------
----------
create	table	#File_LabelOnly
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
create	table	#File_BackupHeader	-- взято из процедуры master.dbo.sp_can_tlog_be_applied
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
create	table	#File_BackupHeader1
(	Step			smallint
	,databaseName		nvarchar ( 128 ) )
----------
create	table	#File_FileListOnly
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
create	table	#File_FileListOnly1
(	Step			smallint
	,LogicalName		nvarchar ( 128 )  )
----------
set	@sBackupSign=		'_backup_'
----------
if		convert ( varchar ( 10 ) , serverproperty ( 'ProductVersion' ) )	like	'%.%'	-- проверяем версию сервера
	and	10<	left ( convert ( varchar ( 10 ) , serverproperty ( 'ProductVersion' ) ) , charindex ( '.' , convert ( varchar ( 256 ) , serverproperty ( 'ProductVersion' ) ) ) - 1 )
begin
	alter	table	#File_LabelOnly	add
		MirrorCount		int	-- в документации поле называется Mirror_Count
		,IsCompressed		bit
----------
	alter	table	#File_BackupHeader	add
		CompressedBackupSize	bigint
		,containment		tinyint
----------
	alter	table	#File_FileListOnly	add
		TDEThumbprint		varbinary ( 32 )
----------
	set	@sPattern=		'%2[0-9][0-9][0-9][_][0-1][0-9][_][0-3][0-9][_][0-2][0-9][0-5][0-9][0-5][0-9][_][0-9][0-9][0-9][0-9][0-9][0-9][0-9].%'	-- маска имени backup файла из MaintenancePlan sql 2012
end
else
begin
	alter	table	#File_LabelOnly	add
		Mirror_Count		int
----------
	set	@sPattern=		'%2[0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9].%'								-- маска имени backup файла из MaintenancePlan sql 2005
end











----------
/*if	exists	( select
			1
		from
			( select
				Sequence=	DENSE_RANK()	over	( partition	by	Parent	order	by	DB )
			from
				#Body )	е
		where
			Sequence<>	1 )
begin
	set	@sMessage=	'Ошибка подачи параметров в атрибутах: база данных либо пуста во всей группе, либо заполнена во всей группе'
	goto	error
end*/
----------
update
	s
set
	s.Server=	ss.Server
	,s.Suit=	case	s.Suit
				when	''	then	replace ( replace ( replace ( replace ( convert ( varchar ( 24 ) , getdate() , 121 ) , '-' , '' ) , ' ' , '' ) , ':' , '' ) , '.' , '' )
				else			s.Suit
			end
from
	#Suit		s
	,#SuitServer	ss
where
	ss.Parent=	s.Id
----------
set	@cTemp=	cursor	local	fast_forward	for
			select
				Parent
				,Server
				,quotename ( Server )
			from
				#Body
			where
				DB	is	null
			group	by
				Parent
				,Server
----------
open	@cTemp
----------
while	1=	1
begin
	fetch	next	from	@cTemp	into	@iParent,	@sServer,	@sServerQuoted
	if	@@fetch_status<>	0	break
----------
	set	@sScript=	'
insert
	#DBss	( Parent,	Server,	DB,	DBQuoted )
select
	'+	convert ( varchar ( 256 ) , @iParent )+	'
	,'''+	@sServer+	'''
	,name
	,quotename ( name )
from
	'+	case
			when	@sServer	is	null	then	'sysdatabases'
			else						'openquery ( '+	@sServerQuoted+	',	''select	name	from	sysdatabases	order	by	name'' )'
		end+	'
order	by
	name'
----------
	exec ( @sScript )	
end
----------
deallocate	@cTemp
----------
update
	b
set
	b.FromDB=	case
				when	b.Action	like	'backup%'	then	isnull ( d.DB , b.FromDB )
				else							b.FromDB
			end
	,b.ToDB=	case
				when	b.Action	like	'backup%'	then	b.ToDB
				else							isnull ( d.DB , b.ToDB )
			end
	,b.ToFile=	case
				when	b.Action	like	'backup%'	then	replace ( isnull ( b.Server , @@servername ) , '\' , '!' )	-- ***по имени файла нельзя понять, этот файл с linked server или с локального сервера с таким же именем
										+	'_'
										+	b.DB
										+	'_'
										+	@sBackupSign
										+	'_'
										+	isnull ( s.Suit , replace ( replace ( replace ( replace ( convert ( varchar ( 24 ) , getdate() , 121 ) , '-' , '' ) , ' ' , '' ) , ':' , '' ) , '.' , '' ) )
										+	case
												when	s.Suit	is	null	then	'_'+	replace ( str ( b.Id , 4 ) , ' ' , '0' )
												else					''
											end
										+	'.'
										+	case
												when	b.Action	like	'%database'	then	@sExtBak
												else							@sExtTrn
											end
				else							b.ToFile
			end
from
	#Body	b
	left	join	#DBss	d	on
		d.Parent=	b.Parent
	and	d.Server=	b.Server
	inner	join	#Suit	s	on
		s.Id=		b.Parent
where
		b.FromDB	is	null
	or	b.ToDB		is	null
	or	b.ToFile	is	null
----------
set	@cTemp=	cursor	local	static	FORWARD_ONLY	read_only	for
			select
				ServerQuoted
				,FromFolder
			from
				#Body
			where
					Action=		'restore'
				and	FromFile	is	null
			group	by
				ServerQuoted
				,FromFolder
			order	by
				ServerQuoted
				,FromFolder
----------
open	@cTemp
----------
while	0<	@@CURSOR_ROWS
begin
	fetch	next	from	@cTemp	into	@sServerQuoted,	@sFromFolder
	if	@@fetch_status<>	0	break
----------
	select	@sExecAtServer=	isnull ( @sServerQuoted+	'...' , '' )+	'xp_dirtree'	-- xp_cmdshell не даёт преимуществ, т.к. wildcard в dir работают на длинные и короткие имена файлов, что может перепутать результаты, например, для имени оканчивающегося на ~1 и 1
----------
	insert	#BackupDir	( FileName,	Level,	IsFile )
	exec	@sExecAtServer
			@sFromFolder
			,0
			,1
	if	@@error<>	0
	begin
		set	@sMessage=	'Ошибка получения каталога файлов'
		goto	error
	end
----------
	delete				-- оставляем только файлы, относящиеся к backup заданной базы исходя из ЧАСТИЧНОГО совпадения имени файла, проблема будет, если одно имя полностью содержит другое, например, DB и DB1
		#BackupDir
	where
			FileName	not	like '%.'+	@sExtBak
		and	FileName	not	like '%.'+	@sExtTrn
----------
	update
		#BackupDir
	set
		ServerQuoted=	@sServerQuoted
		,Folder=	@sFromFolder
	where
			ServerQuoted=	''			-- исключая null при отсутствии сервера
		and	Folder=		''
	if	@@error<>	0
	begin
		set	@sMessage=	'Ошибка получения каталога файлов'
		goto	error
	end
end
----------
if	0<	@@CURSOR_ROWS
begin
	select	@iSequenceMax=	max ( Id )	from	#Body
----------
	insert
		#Body	( Id,	Parent,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFile,	IsCompressed,	IsCopyOnly,	Access )
	select
		@iSequenceMax+	d.Sequence
		,b.Parent

		,b.Action
		,b.FromServer
		,b.FromDB
		,b.FromFolder
		,d.FileName
		,b.ToServer
		,b.ToDB
		,b.ToFolder
		,b.ToFile
		,b.IsCompressed
		,b.IsCopyOnly
		,b.Access
	from
		#Body		b
		,( select
			*
			,Sequence=	row_number()	over	( partition	by	ServerQuoted,	Folder	order	by	FileName )
		from
			#BackupDir )	d
	where
			(	b.ServerQuoted=	d.ServerQuoted
			or	isnull ( b.ServerQuoted , d.ServerQuoted )	is	null )
		and	b.Folder=	d.Folder
		and	d.FileName	like	'%'+	b.DB+	'%'	-- файлы, относящиеся к backup заданной базы исходя из ЧАСТИЧНОГО совпадения имени файла, проблема будет, если одно имя полностью содержит другое, например, DB и DB1
		and	b.FileName	is		null
		and	1<		d.Sequence			-- добавляем записи дублируя все поля, кроме FileName
----------
	update
		b
	set
		FromFile=	d.FileName
	from
		#Body		b
		,( select
			*
			,Sequence=	row_number()	over	( partition	by	ServerQuoted,	Folder	order	by	FileName )
		from
			#BackupDir )	d
	where
			(	b.ServerQuoted=	d.ServerQuoted
			or	isnull ( b.ServerQuoted , d.ServerQuoted )	is	null )
		and	b.Folder=	d.Folder
		and	d.FileName	like	'%'+	b.DB+	'%'	-- файлы, относящиеся к backup заданной базы исходя из ЧАСТИЧНОГО совпадения имени файла, проблема будет, если одно имя полностью содержит другое, например, DB и DB1
		and	b.FileName	is	null
		and	d.Sequence=	1				-- существующую одну запись обновляем
end
----------
deallocate	@cTemp	-- закрывать после повторного обращения к @@CURSOR_ROWS
----------
set	@cTemp=	cursor	local	fast_forward	for
			select
				Id
				,ServerQuoted
				,Folder
				,FileName
				,DB
			from
				#Body
			where
				Action=		'restore'
			order	by
				ServerQuoted
				,FromFolder
				,FileName
				,DB
----------
open	@cTemp
----------
while	1=	1
begin
	fetch	next	from	@cTemp	into	@iStep,	@sServerQuoted,	@sFromFolder,	@sFromFile,	@sDB
	if	@@fetch_status<>	0	break
----------
	set	@sBackupDirFileQuoted=	''''+	@sFromFolder+	@sFromFile+	''''
----------
	set	@sScriptTemp=	'restore	labelonly	from	disk=	'+	@sBackupDirFileQuoted
----------
	if	@bDebug=	1
		print	( @sScriptTemp )
----------
	truncate	table	#File_LabelOnly					-- из-за наличия continue в тексте стирать лучше до
----------
	select	@sExecAtServer=	isnull ( @sServerQuoted+	'...' , '' )+	'sp_executesql'
----------
	insert	#File_LabelOnly							-- лог jobа покажет ошибку на залоченном файле, который, например, ещё недоразжат архиватором
	exec	@sExecAtServer							-- датасет от restore нельзя подавать в insert
			@sScriptTemp
	select	@iError=	@@Error
		,@iRowCount=	@@RowCount
	if	@iError<>	0	or	@iRowCount<	1
	begin
		update
			#Body
		set
			Message=	isnull ( Message+	'; ',	'' )+	'ошибка restore labelonly'
		where
			id=	@iStep
	end
----------
	set	@sScriptTemp=	'restore	headeronly	from	disk=	'+	@sBackupDirFileQuoted
----------
	if	@bDebug=	1
		print	( @sScriptTemp )
----------
	truncate	table	#File_BackupHeader
----------
	insert	#File_BackupHeader
	exec	@sExecAtServer							-- датасет от restore нельзя подавать в insert
			@sScriptTemp
	select	@iError=	@@Error
		,@iRowCount=	@@RowCount
	if	@iError<>	0	or	@iRowCount<	1
	begin
		update
			#Body
		set
			Message=	isnull ( Message+	'; ',	'' )+	'ошибка restore headeronly'
		where
			id=	@iStep
	end
----------
	insert
		#File_BackupHeader1	( Step,	databaseName )
	select
		@iStep
		,databaseName
	from
		#File_BackupHeader
	group	by
		databaseName
----------
/*
	select
		@iBackupType=		bh.BackupType			-- 1=bak,2=trn
		,@sDBLive=		sd1.name
		--,@sDBStandby=		isnull ( sd2.name , sd1.name )
		,@dFirstLSN=		bh.FirstLSN
		,@databaseBackupLSN=	bh.databaseBackupLSN
	from
		#File_BackupHeader	bh
		inner	join	server...sysdatabases	sd1	on
			sd1.name=	bh.databaseName
		left	join	server...sysdatabases	sd2	on
			sd2.name=	@sDBLogShippedSign+	bh.databaseName
	where
			isnull ( sd2.name , sd1.name )=	@sDBStandby
*/

--***проверить совпадение @iBackupType и расширения файла












end
----------
deallocate	@cTemp
----------
update
	bN
set
	bN.Message=	isnull ( bN.Message+	'; ',	'' )+	'шаг дублируется'	-- исключаем дублирующиеся шаги из обработки
from
	#Body	b
	inner	join	#Body	bN	on
		bN.Parent=	b.Parent
	and	b.Id<		bN.Id
	and	( bN.Action=	b.Action	or	isnull ( bN.Action,	b.Action )	is	null )
	and	( bN.FromServer=b.FromServer	or	isnull ( bN.FromServer,	b.FromServer )	is	null )
	and	( bN.FromDB=	b.FromDB	or	isnull ( bN.FromDB,	b.FromDB )	is	null )
	and	( bN.FromFolder=b.FromFolder	or	isnull ( bN.FromFolder,	b.FromFolder )	is	null )
	and	( bN.FromFile=	b.FromFile	or	isnull ( bN.FromFile,	b.FromFile )	is	null )
	and	( bN.ToServer=	b.ToServer	or	isnull ( bN.ToServer,	b.ToServer )	is	null )
	and	( bN.ToDB=	b.ToDB		or	isnull ( bN.ToDB,	b.ToDB )	is	null )
	and	( bN.ToFolder=	b.ToFolder	or	isnull ( bN.ToFolder,	b.ToFolder )	is	null )
	and	( bN.ToFile=	b.ToFile	or	isnull ( bN.ToFile,	b.ToFile )	is	null )
	and	( bN.IsCopyOnly=b.IsCopyOnly	or	isnull ( bN.IsCopyOnly,	b.IsCopyOnly )	is	null )
	left	join	#Body	bP	on
		bP.Parent=	b.Parent
	and	b.Id<		bP.Id	and	bP.Id<		bN.Id
	and	(	( bP.Action<>	b.Action	and	isnull ( bP.Action,	b.Action )	is	not	null )
		or	( bP.FromServer<>b.FromServer	and	isnull ( bP.FromServer,	b.FromServer )	is	not	null )
		or	( bP.FromDB<>	b.FromDB	and	isnull ( bP.FromDB,	b.FromDB )	is	not	null )
		or	( bP.FromFolder<>b.FromFolder	and	isnull ( bP.FromFolder,	b.FromFolder )	is	not	null )
		or	( bP.FromFile<>	b.FromFile	and	isnull ( bP.FromFile,	b.FromFile )	is	not	null )
		or	( bP.ToServer<>	b.ToServer	and	isnull ( bP.ToServer,	b.ToServer )	is	not	null )
		or	( bP.ToDB<>	b.ToDB		and	isnull ( bP.ToDB,	b.ToDB )	is	not	null )
		or	( bP.ToFolder<>	b.ToFolder	and	isnull ( bP.ToFolder,	b.ToFolder )	is	not	null )
		or	( bP.ToFile<>	b.ToFile	and	isnull ( bP.ToFile,	b.ToFile )	is	not	null )
		or	( bP.IsCopyOnly<>b.IsCopyOnly	and	isnull ( bP.IsCopyOnly,	b.IsCopyOnly )	is	not	null ) )
where
		bP.Id	is	null
----------









--***оставить только полные Suit, например, для баз dba+dbb+dbc файлы dba1+dbb1+dbc1 образуют полный suit, а dba2+dbb2 неполный, который должен ждать появления dbc2 для своей полноты. неполные наборы поместить в список warning



--***внутри каждого Suit базы отсортировать подряд для минимизации файловых операций






















select * from #BackupDir
select * from #Body




----------
set	@cSuit=	cursor	local	fast_forward	for
			select
				Id
				,Suit
				,Server
				,quotename ( Server )
				,IsAsyncronous
			from
				#Suit
			order	by
				Id
----------
open	@cSuit
----------
while	1=	1
begin
	fetch	next	from	@cSuit	into	@iParent,	@sSuit,	@sServer,	@sServerQuoted,	@bIsAsyncronous
	if	@@fetch_status<>	0	break
----------
	select	@sPostixUnique=		replace ( replace ( replace ( replace ( convert ( varchar ( 24 ) , getdate() , 121 ) , '-' , '' ) , ' ' , '' ) , ':' , '' ) , '.' , '' )
		,@sProcName=		quotename ( '##'+	@sDBLogShippedSign+	'_PlanBR_'+	replace ( @@servername , '\' , '!' )+	@sPostixUnique )	-- ! для единообразия с именем файла
		,@bHasFileworkOnRestore=null
		,@sScript=		'create	proc	'+	@sProcName+	'
	@sDBList	varchar ( 8000 )	output
as
set	@sDBList=	null'
----------

--***передавать в процедуру BodyId

	set	@cBodySteps=	cursor	local	forward_only	for
					select
						b.Id
						,b.Action
						,b.FromServer
						,b.FromDB
						,b.FromFolder
						,b.FromFile
						,b.ToServer
						,b.ToDB
						,b.ToFolder
						,b.ToFile
						,b.IsCompressed
						,b.IsCopyOnly
						,b.Access
						,b.Server
						,b.ServerQuoted
						,b.DB
						,b.DBQuoted
						,b.Extension
						,DBLive=	b.DB
						,DBStandBy=	case
									when	b.Access	is	null	then	''
									else						@sDBLogShippedSign
								end+	b.DB
						,AfterDBChange=	case
									when		bP.Action<>	b.Action
										or	bP.DB<>		b.DB
										or	bP.Id	is	null	then	1
									else						0
								end
						,BeforeDBChange=case
									when		bN.Action<>	b.Action
										or	bN.DB<>		b.DB
										or	bN.Id	is	null	then	1
									else						0
								end
					from
						( select
							*
							,Sequence2=	row_number()	over	( partition	by	Parent	order	by	Id )
						from
							#Body )	b
						left	join	( select
									*
									,Sequence2=	row_number()	over	( partition	by	Parent	order	by	Id )
								from
									#Body )	bP	on
							bP.Parent=	b.Parent
						and	bP.Sequence2=	b.Sequence2-	1
						left	join	( select
									*
									,Sequence2=	row_number()	over	( partition	by	Parent	order	by	Id )
								from
									#Body )	bN	on
							bN.Parent=	b.Parent
						and	bN.Sequence2=	b.Sequence2+	1
					where
							b.Parent=	@iParent
						and	b.Message	is	null	-- если там только ошибки, а не предупреждения
					order	by
						b.Id
----------
	open	@cBodySteps
----------
	while	1=	1
	begin
		fetch	next	from	@cBodySteps	into	@iStep,	@sAction,	@sFromServer,	@sFromDB,	@sFromFolder,	@sFromFile,	@sToServer,	@sToDB,	@sToFolder,	@sToFile,	@bIsCompressed,	@bIsCopyOnly,	@sAccess,	@sServer,	@sServerQuoted,	@sDB,	@sDBQuoted,	@sExtension,	@sDBLive,	@sDBStandBy,	@bAfterDBChange,	@bBeforeDBChange
		if	@@fetch_status<>	0	break
----------
		select
			@iDBCount=	count (	distinct	DB )
		from
			#Body
		where
			Parent=		@iParent
----------
		if	@sAction=	'restore'
		begin
			select
				@bHasFileworkOnRestore=	1
				,@sScript=		@sScript+	'
----------
declare	@iFSO		int
	,@iError	int
----------
EXEC	sp_OACreate	''Scripting.FileSystemObject'',	@iFSO	OUT'
			where
				@bHasFileworkOnRestore	is	null
----------

/*			if	@bIsCoupled=	1
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
			end*/
----------

-- создать базу, если она не существует и поместить её в message?



--@sDBStandby	довосстанавливаем(передавая исходное имя), копируем в @sDBLive	с гуидным именем
--@sDBLive	блокируем для восстановления, читаем, пишем			с исходным именем




--			if	@@RowCount=	1						-- правильно распознали название базы в имени файла, например, 'DB' и 'DB1', здесь 1- часть названия базы или разделитель в имени файла?
			begin
				select	@sLockName=	upper ( @sDBLive )+	@sProjectSign	-- сравнивается в бинарном виде, а одна и та же база может на разных серверах иметь имя в разном регистре
----------
				select
					@sScript=	@sScript+	'
----------
exec	@iError=	/*master..*/sp_getapplock				-- блокируем ресурс; если нужно запретить копирование в активированную базу, то блокировку нужно запросить с @LockMode=Exclusive, например, при выполнении длительного отчёта
				@Resource=	'''+	@sLockName+	'''	-- блокировки учитываются относительно текущей базы, поэтому и проверять их нужно относительно заданной базы, базу можно задать в самом execute
				,@LockMode=	''Shared''
				,@LockOwner=	''Session''
				,@LockTimeout=	0
if	@@Error<>	0	or	@iError<>	0
begin
	set	@sDBList=	isnull ( @sDBList , '''' )+	'''+	@sDBLive+	@sDBDelimeter+	'''
	goto	skip_'+	upper ( @sDBLive )+	'
end'
				where
					@bAfterDBChange=	1
----------
				set	@bRestoring=	1
/*

***нужна проверка, что если есть более поздний .bak, то предыдущие .trn не восстанавливать, учитывая то, что некоторые могут находиться в suit

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
*/
----------
				--if	@bRestoring=	1
				begin
					select	@sBackupDirFileQuoted=	''''+	@sFromFolder+	@sFromFile+	''''
						,@sExecAtServer=	isnull ( @sServerQuoted+	'...' , '' )+	'sp_executesql'
						,@sScriptTemp=		'restore	filelistonly	from	disk=	'+	@sBackupDirFileQuoted+	'	with	nounload'
----------
					if	@bDebug=	1
						print	( @sScriptTemp )
----------
					truncate	table	#File_FileListOnly
----------
					insert	#File_FileListOnly
					exec	@sExecAtServer
							@sScriptTemp
					if	@@error<>	0	continue	-- битый список файлов?
----------
					set	@sScriptTemp=	'
select
	sdb.dbid
	,saf.fileid
	,sdb.name
	,LogicalName=	saf.name
	,saf.filename
	,TempName=	replace ( convert ( varchar ( 36 ) , newid() ) , ''-'' , '''' )	-- получаем гуид на той стороне
from
	sysdatabases	sdb
	,sysaltfiles	saf
where
		sdb.name	like	''%'+	@sDB+	'%''
	and	saf.dbid=	sdb.dbid'
----------
					truncate	table	#Server_DBFiles
----------
					insert	#Server_DBFiles	( dbid,	fileid,	DBName,	LogicalName,	filename,	TempName )
					exec	@sExecAtServer
							@sScriptTemp
----------
					truncate	table	#Generation_Files
----------
					insert
						#Generation_Files	( DBStandby,	DBLive,	StandbyFileName,	LiveFileName,	TempFileName,	IsDBStandbyCreate )
					select
						case
							when	@sAccess	is	not	null	then	isnull ( stby.DBName , @sDBLogShippedSign+	live.DBName )
							else							live.DBName
						end
						,live.DBName
						,case
							when	@sAccess	is	not	null	then	isnull ( stby.filename , left ( live.filename , len ( live.filename )-	charindex ( '\' , reverse ( live.filename ) )+	1 )+	@sDBLogShippedSign+	right ( live.filename , charindex ( '\' , reverse ( live.filename ) )-	1 ) )
							else							live.filename
						end
						,live.filename
						,live.TempName
						,case
							when	@sAccess	is	not	null	and	stby.DBName	is	null	then	1
							else												0
						end
					from
						#File_FileListOnly	flo
						left	join	#Server_DBFiles	live	on
							live.LogicalName=	flo.LogicalName
						and	live.DBName=		@sDB
						left	join	#Server_DBFiles	stby	on
							stby.LogicalName=	flo.LogicalName
						and	(	@sAccess	is		null	and	stby.DBName=	@sDB
							or	@sAccess	is	not	null	and	stby.DBName=	@sDBLogShippedSign+	@sDB )
----------
					set	@sSQLDir=	convert ( nvarchar ( 260 ) , SERVERPROPERTY ( 'instancedefaultdatapath' ) )
----------
					if	@sExtension=	@sExtBak
					begin
						set	@sScript=	@sScript+	'
----------
restore	database
	'+	@sDBStandby+	'
from
	disk=	'+	@sBackupDirFileQuoted+	'
with
	standby=	'''+	@sFromFolder+	@sDBStandby+	'.TUF'',
	nounload,
	replace,
	stats=	100'
----------

--select * from #Generation_Files
select * from #File_FileListOnly
select * from #Server_DBFiles


						select
							@sScript=	@sScript+	'
	,move	'''+	flo.LogicalName+	'''	to	'''+	isnull ( left ( saf.filename , len ( saf.filename )-	charindex ( '\' , reverse ( saf.filename ) )+	1 ) , @sSQLDir )	-- база может восстанавливаться под другим названием
								+	case	@sDBLive
										when	@sDBStandby	then	''
										else				@sDBLogShippedSign
									end
								+	right ( flo.PhysicalName , charindex ( '\' , reverse ( flo.PhysicalName ) )-	1 )+	''''	-- вырезать имя файла из полного пути
						from
							#File_FileListOnly	flo
							left	join	#Server_DBFiles	saf	on
								saf.LogicalName=	flo.LogicalName
							and	saf.DBName=		@sDB
					end
					else
						if	@sExtension=	@sExtTrn
						begin
							/*if	not	exists	( select
											1
										from
									***		sys.master_files
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
	standby=	'''+	@sFromFolder+	@sDBStandby+	'.TUF'''
						end
				end
----------
				set	@sScript=	@sScript+	'
----------
'+	case	@bRestoring		-- даже невосстанавливаемые бекапы нужно стирать, т.к. при запуске с теми же параметрами файлы снова начнут обрабатываться; о сохранности файлов для архива заботится внешний процесс, например, через хардлинк
		when	1	then	'if	@@Error=	0
	'
		else			''
	end+	'exec	sp_OAMethod	@iFSO,	''DeleteFile'',	null,	'+	@sBackupDirFileQuoted+	',	1'	-- ***не стирать, если из этого файла нужно восстанавливать ещё базы с другим именем на этом же сервере
----------
				select
					@sScript=	@sScript+	'
----------
alter	database	'+	@sDBStandby+	'	set	offline'
				where
					@sAccess	is	not	null
----------
				select
					@sScript=	@sScript+	'
----------
exec	sp_OAMethod
		@iFSO
		,''CopyFile''
		,'''+	isnull ( @sSQLDir+	StandbyFile,	StandbyFileName )+	'''
		,'''+	isnull ( @sSQLDir,	StandByPath )+	TempFileName+	''''	--вырезать имя файла из полного пути и копируем файлы для live базы, но с другим именем, чтобы live базу не останавливать
				from
					#Generation_Files
				where
					@sAccess	is	not	null
----------
				select
					@sScript=	@sScript+	'
----------
alter	database	'+	@sDBStandby+	'	set	online'
				where
					@sAccess	is	not	null
----------
				select
					@sScript=	@sScript+	'
----------
alter	database	'+	@sDBLive+		'	set	offline'
				where
					@sAccess	is	not	null
----------
				select
					@sScript=	@sScript+	'
----------
exec	sp_OAMethod	@iFSO,	''DeleteFile'',	'''+	@sSQLDir+	LiveFile+	''''	--переименовываем скопированные под другим именем файлы в файлы Live базы
				from
					#Generation_Files
				where
					@sAccess	is	not	null
----------
				select
					@sScript=	@sScript+	'
----------
exec	sp_OAMethod	@iFSO,	''MoveFile'',	'''+	@sSQLDir+	TempFileName+	''',	'''+	@sSQLDir+	LiveFile+	''''	--переименовываем скопированные под другим именем файлы в файлы Live базы
				from
					#Generation_Files
				where
					@sAccess	is	not	null
----------
				select
					@sScript=	@sScript+	'
----------
alter	database	'+	@sDBLive+		'	set	online'
				where
					@sAccess	is	not	null
----------
				select						-- базе в standby нельзя сделать sp_changedbowner
					@sScript=	@sScript+	'
----------
exec	'+	@sDBLive+	'..sp_changedbowner	'''+	'dbo'/*@sOwner*/+	''''
				where
					@sAccess	is	not	null
			end
----------
			select
				@sScript=	@sScript+	'
----------
exec	@iError=	sp_releaseapplock		-- отпускаем блокировку
				@Resource=	'''+	@sLockName+	'''
				,@LockOwner=	''Session''
skip_'+	upper ( @sDBLive )+	':'
			where
				@bBeforeDBChange=	1
----------
			select
				@sScript=	'
----------
exec	'+	@sServerQuoted+	'...sp_executesql
		@statement=	'''+	replace ( @sScript , '''' , '''''' )+	'''
		,@params=	N''@sDBList	varchar ( 8000 )	out''
		,@s=		@sDBList	out'
			where
				@sServerQuoted	is	not	null
----------










		end
----------------------------------------------------------------------------------------------------
		else
			if		@sAction	like	'backup database'
				or	@sAction	like	'backup log'
			begin
				select	@dtMoment=	getdate()
					,@sCutoff=	str ( year ( @dtMoment ) , 4 )
						+	'_'
						+	replace ( str ( month ( @dtMoment ) , 2 ) , ' ' , '0' )
						+	'_'
						+	replace ( str ( day ( @dtMoment ) , 2 ) , ' ' , '0' )
						+	'_'
						+	replace ( replace ( right ( convert ( varchar ( 23 ) , @dtMoment , 121 ) , 12 ) , ':' , '' ) , '.' , '_' )
						--+	'0000'
----------
				select	@sBackupInfo=		@sCutoff
							+	'|'+	convert ( varchar ( 128 ) , convert ( varchar ( 16 ) , SERVERPROPERTY ( 'ProductVersion' ) )
							+	'|'+	convert ( varchar ( 8 ) , SERVERPROPERTY ( 'EngineEdition' ) )
							+	'|'+	@@servername+	isnull ( '.'+	db_name()+	'.'+	schema_name ( OBJECTPROPERTY ( @@procid,'OwnerId' ) )+	'.'+	object_name ( @@procid ) , '' )
							+	'|'+	convert ( varchar ( 2 ) , @@NESTLEVEL )
							+	isnull ( '|'+	app_name() , '' ) )
					,@sBackupInfoStr=	replace ( @sBackupInfo , '''' , '''''' )	-- чтобы при склеивании команды символ сохранился
----------
				set	@cTemp=	cursor	local	fast_forward	for
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
								#Body	t
								left	join	sysservers	ss	on
									ss.srvname=	t.Server
								--and	ss.isremote=	0
							where
									t.Parent=	@iParent
							group	by
								t.Server
								,t.ServerQuoted
								,ss.srvname
								,ss.isremote
							order	by
								IsAbsent	desc
								,min ( t.Id )
----------
				open	@cTemp
----------
				while	1=	1
				begin
					fetch	next	from	@cTemp	into	@sServer,	@sServerQuoted,	@bServerAbsent
					if	@@fetch_status<>	0	break
----------
					if	@bServerAbsent=	1
					begin
						select
							@sDBListOut=	isnull ( @sDBListOut , '' )+	DB+	@sDBDelimeter
						from
							#Body
						where
								Parent=		@iParent
							and	Server=		@sServer
						group	by
							DB
						order	by
							min ( Id )
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
		DISK=	'''+	ToFolder+	ToFile+	'''
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
						#Body
					where
							Parent=		@iParent
						and	(	Server=	@sServer
							or	isnull ( Server , @sServer )	is	null )
					order	by
						Id






























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
				deallocate	@cTemp
			end
	end
----------
	close	@cBodySteps
----------
	select
		@sScript=		@sScript+	'
----------
exec	sp_OADestroy	@iFSO'
	where
		@bHasFileworkOnRestore=	1
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
--	exec	( @sScript )



----------
/*
	exec	@sProcName
			@sDBList=	@sDBListOut2	output
*/
----------
	set	@sDBListOut=	@sDBListOut+	isnull ( @sDBListOut2+	@sDBDelimeter , '' )
end
----------
deallocate	@cBodySteps
----------
deallocate	@cSuit

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
--if	cursor_status ( 'variable' , '@cTemp' )<>	-2	deallocate	@cTemp

--drop	table
--	#BackupDir