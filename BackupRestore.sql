GO
RECONFIGURE
GO
exec	sp_configure 'show advanced options',		1
exec	sp_configure 'Ole Automation Procedures',	1
GO
RECONFIGURE
GO
use	master
go
if	object_id ( 'dbo.BackupRestore',	'p' )	is	null
	exec	( 'create	proc	dbo.BackupRestore	as	select	ObjectNotCreated=	1/0' )
go
alter	proc	dbo.BackupRestore	-- бекап и восстановление баз данных
	@tPlan		text			-- план обработки бекапа в XML
	,@tMessage	text=	null	output	-- список сообщений/ошибок в XML, например, что БД есть в плане, но она не обработалась
--(c) 2017-2020 TUnknown License: public domain/cc0
as
-- учитывать возможные одинарные кавычки в именах файлов
-- создать эту процедуру как временную на linked сервере и подать в неё xml параметры, относящиеся только к тому серверу




/*363B1BEF8FA34873824C29D1EBC10C79_1*/		--при вызове на другом сервере через linked server здесь должно быть drop	proc	dbo.BackupRestore





set	nocount	on
declare	@iError			int
	,@iRowCount		int
	,@sMessage		varchar ( 256 )
	,@bDebug		bit=1	-- 1=включить отладочные сообщения

	,@iOLEConnection	int
	,@sMessage1		VARCHAR ( 4000 )
	,@sConnection		VARCHAR ( 4000 )
	,@sLogin		varchar ( 128 )
	,@sPwd			varchar ( 128 )
	,@iDumb			bigint

--	,@sBackupFile		varchar ( 256 )
	,@sBackupDirFileQuoted	varchar ( 256 )

--	,@iBackupType		int
	,@sDBStandby		varchar ( 256 )
	,@sDBLive		varchar ( 256 )
	,@sCutoff		varchar ( 256 )

--	,@dFirstLSN		numeric ( 25,	0 )
--	,@databaseBackupLSN	numeric ( 25,	0 )

	,@sScript		nvarchar ( max )			-- не совместимо с sql 2000
	,@sScriptPart		nvarchar ( max )
	,@sScriptTemp		nvarchar ( max )

	,@cTemp			cursor
	,@cSuit			cursor
	,@cBodySteps		cursor

	,@bRestoring		bit
--	,@bStopWaiting		bit
--	,@bQuorum		bit
	,@bAfterDBChange	bit
	,@bBeforeDBChange	bit
	,@bAfterServerChange	bit
	,@bBeforeServerChange	bit

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
	,@sExecAtServer		nvarchar ( 256 )
	,@sLockName		nvarchar ( 255 )

	,@sProjectSign		varchar ( 32 )
	,@sProcName		sysname

--	,@iDBCount		int		-- число баз в backup, восстанавливаемых синхронно
	,@dtMoment		datetime
	,@dtNow			datetime
	,@sNow			varchar ( 23 )
	,@sNow1			varchar ( 23 )
	,@sDBDelimeter		varchar ( 2 )

	,@iXML			integer
	,@iParent		smallint

	,@sSQLDirDefault	nvarchar ( 260 )
	,@sSQLDir		nvarchar ( 260 )	-- (		restore	)каталог с файлами(mdf/ndf/ldf) баз данных MSSQL
	,@sDBList		varchar ( 8000 )
	,@bIsCoupled		bit		-- ->Cutoff( backup	restore	)backup=именовать базы групповым признаком;restore=базы обрабатывать вместе, если одной не хватает, то не выполняться; разницей в начале снятия бекапа пренебрегаем
	,@sOwner		sysname		-- (		restore	)владелец базы задаётся если база восстанавливается под logshipping без readonly

	,@bIsAsyncronous	bit

	,@sSuit			sysname

	,@iStep			smallint
	,@sAction		varchar ( 256 )	-- команда BACKUP DATABASE/BACKUP LOG/restore
	,@sFromServer		sysname
	,@sFromDB		sysname
	,@sFromFolder		sysname
	,@sFromFile		sysname
	,@sToServer		sysname
	,@sToDB			sysname
	,@sToFolder		sysname
	,@sToFolderFinal	sysname
	,@sToFile		sysname
	,@bIsCompressed		bit		-- ( backup		)использовать сжатие sqlserver, хуже сжимает, чем архиватор; сильное сжатие нужно для передачи по медленной сети, когда затраты на сжатие окупаются ускорением передачи
	,@bIsCopyOnly		bit		-- ( backup		)полный бекап, не нарушающий цепочку backup log
	,@sServer		sysname
	,@sServerQuoted		sysname
	,@sDB			sysname
	,@sDBQuoted		sysname
	,@sAccess		varchar ( 256 )

	,@iSequenceMax		smallint

	,@dLSNFirst		numeric ( 25,	0 )
	,@dLSNLast		numeric ( 25,	0 )

	,@sServerVersion	varchar ( 256 )
	,@iServerVersion	tinyint

	,@xPart			xml
	,@sXML			varchar ( max )

	,@sHKLM			varchar ( 32 )
	,@sInstanceKey		nvarchar ( 1024 )
	,@sKey			nvarchar ( 1024 )

	,@sUnique		varchar ( 32 )
----------
select	@bDebug=		1
	,@sExtBak=		'bak'							-- должны быть совместимы с MainenancePlan
	,@sExtTrn=		'trn'
	,@sProjectSign=		'363B1BEF8FA34873824C29D1EBC10C79'
	,@sDBLogShippedSign=	'z'+	@sProjectSign					-- считаем, что база под log shipping имеет уникальный префикс в имени
	,@sDBDelimeter=		';'
	,@sDBListOut=		''

	,@dtNow=		getdate()
	,@sNow=			convert ( char ( 23 ),	@dtNow,	121 )
	,@sNow1=		replace ( replace ( replace ( replace ( @sNow,	'-',	'' ),	' ',	'' ),	':',	'' ),	'.',	'' )

	,@sBackupSign=		'_backup_'
	,@sServerVersion=	convert ( varchar ( 256 ), serverproperty ( 'ProductVersion' ) )
	,@iServerVersion=	isnull ( parsename ( @sServerVersion,	4 ),	parsename ( @sServerVersion,	3 ) )
----------

--*** нужен ли параметр во входящем XML- директории для складывания разных файлов БД при отсутствии её на сервере, т.е. при автосоздании?

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
	,ToFolderFinal	sysname		null		--backup/restore		-- не поддерживается
	,ToFile		sysname		null		--backup
	,IsCompressed	bit		null		--backup
	,IsCopyOnly	bit		null		--backup
	,Moment		datetime	null		--restore
	,ProcName	sysname		null		--backup/restore
,check	( Action	in	( 'backup database',	'backup log',	'restore' ) )	)
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
	,ToFolder	sysname		null		--backup/restore
	,ToFolderFinal	sysname		null		--backup/restore	-- перенос в этот фолдер после backup/restore, для того, чтобы там файлы были незаблокированы длительной операцией	-- не поддерживается
	,ToFile		sysname		null		--backup
	,IsCompressed	bit		null		--backup
	,IsCopyOnly	bit		null		--backup
	,Moment		datetime	null		--restore
	,LSNFirst	numeric ( 25,	0 )	null	--restore
	,LSNLast	numeric ( 25,	0 )	null	--restore
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
					end,	len (	case
								when	Action	like	'backup%'	then	ToFile
								else						FromFile
							end )-	charindex ( '.',	reverse (	case
													when	Action	like	'backup%'	then	ToFile
													else						FromFile
												end ) ) )
	,Extension	as	right (	case
						when	Action	like	'backup%'	then	ToFile
						else						FromFile
					end,	charindex ( '.',	reverse ( case
											when	Action	like	'backup%'	then	ToFile
											else						FromFile
										end ) )-	1 )


,unique	( Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFile,	IsCompressed,	IsCopyOnly,	Moment )
,check	( Action	in	( 'backup database',	'backup log',	'restore' ) )
,check	( Access	in	( 'read',	'write' ) )
/*
,check	( FromDB	is	not	null	or	ToDB		is	not	null )	--/ограничение не работает при автозаполнении параметров; если обрабатывать все доступные базы, то параметр не указывается
,check	( FromFolder	is	not	null	or	ToFolder	is	not	null )	--\
*/
)
----------
if	isnull ( datalength ( @tPlan ),	0 )<	4
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
	#Suit	( Id,	Suit,	IsAsyncronous,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFolderFinal,	ToFile,	IsCompressed,	IsCopyOnly,	Moment )
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
	,ToFolderFinal
	,ToFile
	,IsCompressed
	,IsCopyOnly
	,Moment
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
	,ToFolderFinal	sysname
	,ToFile		sysname
	,IsCompressed	bit
	,IsCopyOnly	bit
	,Moment		datetime )
----------
-- нужна проверка ошибок всех check и unique?
insert
	#Body	( Id,	Parent,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFolderFinal,	ToFile,	IsCompressed,	IsCopyOnly,	Moment,	Access )
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
	,ToFolderFinal
	,ToFile
	,IsCompressed
	,IsCopyOnly
	,Moment
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
	,ToFolderFinal	sysname
	,ToFile		sysname
	,IsCompressed	bit
	,IsCopyOnly	bit
	,Moment		datetime
	,Access		varchar ( 256 ) )
----------
EXEC	@iError=	sp_xml_removedocument	@iXML	-- sql2000+
if	@@Error<>	0	or	@iError<>	0
begin
	select	@sMessage=	'Ошибка XML 2',
		@iError=	-3
	goto	error
end
----------
update
	b
set
	@sMessage=	isnull ( @sMessage,	'' )
		+	case
				when	( b.Action<>	s.Action	or	isnull ( b.Action,	s.Action )	is	null )	and	@sMessage	not	like	'%,Action%'		then	',Action'
				when	b.FromServer<>	s.FromServer									and	@sMessage	not	like	'%,FromServer%'		then	',FromServer'
				when	b.FromDB<>	s.FromDB									and	@sMessage	not	like	'%,FromDB%'		then	',FromDB'
				when	b.FromFolder<>	s.FromFolder									and	@sMessage	not	like	'%,FromFolder%'		then	',FromFolder'
				when	b.FromFile<>	s.FromFile									and	@sMessage	not	like	'%,FromFile%'		then	',FromFile'
				when	b.ToServer<>	s.ToServer									and	@sMessage	not	like	'%,ToServer%'		then	',ToServer'
				when	b.ToDB<>	s.ToDB										and	@sMessage	not	like	'%,ToDB%'		then	',ToDB'
				when	b.ToFolder<>	s.ToFolder									and	@sMessage	not	like	'%,ToFolder%'		then	',ToFolder'
				when	b.ToFolderFinal<>s.ToFolderFinal								and	@sMessage	not	like	'%,ToFolderFinal%'	then	',ToFolderFinal'
				when	b.ToFile<>	s.ToFile									and	@sMessage	not	like	'%,ToFile%'		then	',ToFile'
				when	b.IsCompressed<>s.IsCompressed									and	@sMessage	not	like	'%,IsCompressed%'	then	',IsCompressed'
				when	b.IsCopyOnly<>	s.IsCopyOnly									and	@sMessage	not	like	'%,IsCopyOnly%'		then	',IsCopyOnly'
				when	b.Moment<>	s.Moment									and	@sMessage	not	like	'%,Moment%'		then	',Moment'
				else																						''
			end
	,Action=	isnull ( b.Action,	s.Action )
	,FromServer=	case
				when	isnull ( b.Action,	s.Action )	like	'backup%'	then	nullif ( isnull ( b.FromServer,	s.FromServer ),	@@servername )
				else										isnull ( b.FromServer,	s.FromServer )
			end
	,FromDB=	isnull ( b.FromDB,	s.FromDB )
	,FromFolder=	isnull ( b.FromFolder,	s.FromFolder )+	case
									when	isnull ( b.FromFolder,	s.FromFolder )	not	like	'%\'	then	'\'
									else										''
								end
	,FromFile=	isnull ( b.FromFile,	s.FromFile )
	,ToServer=	case
				when	isnull ( b.Action,	s.Action )	like	'restore'	then	nullif ( isnull ( b.ToServer,	s.ToServer ),	@@servername )
				else										isnull ( b.ToServer,	s.ToServer )
			end
	,ToDB=		isnull ( b.ToDB,	s.ToDB )
	,ToFolder=	isnull ( b.ToFolder,	s.ToFolder )+	case
									when	isnull ( b.ToFolder,	s.ToFolder )	not	like	'%\'	then	'\'
									else										''
								end
	,ToFolderFinal=	isnull ( b.ToFolderFinal,s.ToFolderFinal )+	case
										when	isnull ( b.ToFolderFinal,	s.ToFolderFinal )	not	like	'%\'	then	'\'
										else												''
									end
	,ToFile=	isnull ( b.ToFile,	s.ToFile )
	,IsCompressed=	isnull ( b.IsCompressed,s.IsCompressed )
	,IsCopyOnly=	isnull ( b.IsCopyOnly,	s.IsCopyOnly )
	,Moment=	isnull ( b.Moment,	s.Moment )
from
	#Suit	s
	,#Body	b
where
	b.Parent=	s.Id
----------
set	@sMessage=	nullif ( stuff ( @sMessage,	1,	1,	'' ),	'' )
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
create	table	#BackupDir
(	ServerQuoted		nvarchar ( 128 )	null		default ( '' )
	,Folder			nvarchar ( 260 )	not null	default ( '' )
	,FileName		nvarchar ( 255 )	null
	,Level			tinyint			null
	,IsFile			bit			null )
----------
create	table	#Server_DBFiles
(	dbid			integer
	,fileid			integer
	,DBName			sysname
	,LogicalName		nvarchar ( 260 )
	,filename		nvarchar ( 260 )
	,TempName		varchar ( 32 )

	,Path		as	left ( filename,	len ( filename )-	charindex ( '\',	reverse ( filename ) )+	1 )
	,File1		as	right ( filename,	charindex ( '\',	reverse ( filename ) )-	1 )	)
----------
create	table	#Generation_Files
(	DBStandby		varchar ( 256 )
	,DBLive			varchar ( 256 )
	,StandbyFileName	nvarchar ( 260 )
	,LiveFileName		nvarchar ( 260 )
	,TempFileName		nvarchar ( 260 )
	,IsDBStandbyCreate	bit

	,StandbyPath	as	left ( StandbyFileName,	len ( StandbyFileName )-	charindex ( '\',	reverse ( StandbyFileName ) )+	1 )
	,StandbyFile	as	right ( StandbyFileName,charindex ( '\',	reverse ( StandbyFileName ) )-	1 )
	,LivePath	as	left ( LiveFileName,	len ( LiveFileName )-	charindex ( '\',	reverse ( LiveFileName ) )+	1 )
	,LiveFile	as	right ( LiveFileName,	charindex ( '\',	reverse ( LiveFileName ) )-	1 ) )
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
	,BackupSize		numeric ( 20,	0 )	NULL
	,FirstLsn		numeric ( 25,	0 )	NULL
	,LastLsn		numeric ( 25,	0 )	NULL
	,CheckpointLsn		numeric ( 25,	0 )	NULL
	,databaseBackupLsn	numeric ( 25,	0 )	NULL
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
	,ForkPointLSN		numeric ( 25,	0 )	null
	,RecoveryModel		nvarchar ( 60 )		null
	,DifferentialBaseLSN	numeric ( 25,	0 )	null
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
	,Size			numeric ( 20,	0 ) 
	,MaxSize		numeric ( 20,	0 ) 
	,FileID			bigint
	,CreateLSN		numeric ( 25,	0 ) 
	,DropLSN		numeric ( 25,	0 ) 	NULL
	,UniqueID		uniqueidentifier
	,ReadOnlyLSN		numeric ( 25,	0 ) 	NULL
	,ReadWriteLSN		numeric ( 25,	0 ) 	NULL
	,BackupSizeInBytes	bigint
	,SourceBlockSize	int
	,FileGroupID		int
	,LogGroupGUID		uniqueidentifier	NULL
	,DifferentialBaseLSN	numeric ( 25,	0 ) 	NULL
	,DifferentialBaseGUID	uniqueidentifier
	,IsReadOnly		bit
	,IsPresent		bit )
----------
create	table	#File_FileListOnly1
(	Step			smallint
	,LogicalName		nvarchar ( 128 )  )
----------
if	@iServerVersion=	10			-- проверяем версию сервера
begin
	alter	table	#File_LabelOnly		add
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
	if	@iServerVersion=	12
	begin
		alter	table	#File_LabelOnly		add
			MirrorCount		int	-- в документации поле называется Mirror_Count
			,IsCompressed		bit
----------
		alter	table	#File_BackupHeader	add
			CompressedBackupSize	bigint
			,containment		tinyint
			,KeyAlgorithm		nvarchar ( 32 )		-- в документации отсутствуют
			,EncryptorThumbprint	varbinary ( 20 )
			,EncryptorType		nvarchar ( 32 )
----------
		alter	table	#File_FileListOnly	add
			TDEThumbprint		varbinary ( 32 )
----------
		set	@sPattern=		'%2[0-9][0-9][0-9][_][0-1][0-9][_][0-3][0-9][_][0-2][0-9][0-5][0-9][0-5][0-9][_][0-9][0-9][0-9][0-9][0-9][0-9][0-9].%'	-- маска имени backup файла из MaintenancePlan sql 2012
	end
	else
	begin
		alter	table	#File_LabelOnly		add	-- что, если linked сервер версии, несовпадающей с основным?
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
				when	''	then	@sNow1
				else			s.Suit
			end
from
	#Suit		s
	,#SuitServer	ss
where
	ss.Parent=	s.Id



/*
		select
			@sDBListOut=	isnull ( @sDBListOut,	'' )/*+	t.Server*/+	t.DB+	@sDBDelimeter
		from
			#Body	t
			left	join	sysservers	ss	on
				ss.srvname=	t.Server
		where
				t.Parent=	@iParent
			and	(	ss.isremote=	1			-- remote сервера не поддерживаются, только linked
				or	(	t.Server	is	not	null
					and	ss.srvname	is		null ) )
		group	by
			--t.Server					-- группируем по серверам внутри шага
			t.DB
		order	by
			min ( Id )					-- сортировка может не сработать
*/



----------
update
	b
set
	b.FromDB=	case
				when	b.Action	like	'backup%'	then	isnull ( d.name/*зачем это здесь?*/,	b.FromDB )
				else							b.FromDB
			end
	,b.ToDB=	case
				when	b.Action	like	'backup%'	then	b.ToDB
				else							isnull ( d.name,	b.ToDB )
			end
	,b.ToFile=	case
				when	b.Action	like	'backup%'	then	replace ( isnull ( b.Server,	@@servername ),	'\',	'!' )	-- ***по имени файла нельзя понять, этот файл с linked server или с локального сервера с таким же именем
										+	'_'
										+	b.DB
										+	'_'
										+	@sBackupSign
										+	'_'
										+	isnull ( s.Suit,	@sNow1 )
										+	case
												when	s.Suit	is	null	then	'_'+	replace ( str ( b.Id,	4 ),	' ','0' )
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
	inner	join	sysdatabases	d	on		-- ***игнорируем все несуществующие базы, нужно ли автосоздавать их по бекапу?
		d.name=	b.DB
	inner	join	#Suit		s	on
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
	insert	#BackupDir	( FileName,	Level,	IsFile )
	exec	xp_dirtree		-- xp_cmdshell не даёт преимуществ, т.к. wildcard в dir работают на длинные и короткие имена файлов, что может перепутать результаты, например, для имени оканчивающегося на ~1 и 1
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
		#Body	( Id,	Parent,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFolderFinal,	ToFile,	IsCompressed,	IsCopyOnly,	Moment,	Access )
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
		,b.ToFolderFinal
		,b.ToFile
		,b.IsCompressed
		,b.IsCopyOnly
		,b.Moment
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
			or	isnull ( b.ServerQuoted,	d.ServerQuoted )	is	null )
		and	b.Folder=	d.Folder
		and	d.FileName	like	'%'+	b.FromDB+	'%'	-- файлы, относящиеся к backup базы(ToDB может иметь другое имя) исходя из ЧАСТИЧНОГО совпадения имени файла, проблема будет, если одно имя полностью содержит другое, например, DB и DB1
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
			or	isnull ( b.ServerQuoted,	d.ServerQuoted )	is	null )
		and	b.Folder=	d.Folder
		and	d.FileName	like	'%'+	b.FromDB+	'%'	-- файлы, относящиеся к backup базы(ToDB может иметь другое имя) исходя из ЧАСТИЧНОГО совпадения имени файла, проблема будет, если одно имя полностью содержит другое, например, DB и DB1
		and	b.FileName	is	null
		and	d.Sequence=	1				-- существующую одну запись обновляем
end
----------
deallocate	@cTemp	-- закрывать после повторного обращения к @@CURSOR_ROWS
----------





if	@bDebug=	1
begin
	select * from #BackupDir
	select * from #Body
end





-- ***этот запрос нужно выполнить на той стороне?
select	@sHKLM=		'HKEY_LOCAL_MACHINE'
----------
exec	xp_instance_regread
		@rootkey=	@sHKLM
		,@key=		'SOFTWARE\Microsoft\\Microsoft SQL Server\\Instance Names\SQL'	--Определяем ключ инстанса, двойной обратный слеш для пробелов в имени ключа
		,@value_name=	@@SERVICENAME
		,@value=	@sInstanceKey	out
----------
select	@sInstanceKey=	'SOFTWARE\Microsoft\\Microsoft SQL Server\\'+	@sInstanceKey
	,@sKey=			@sInstanceKey+	'\MSSQLSERVER'
	,@sSQLDirDefault=	null
----------
exec	xp_instance_regread
		@sHKLM
		,@sKey
		,N'DefaultData'
		,@sSQLDirDefault	output
----------
if	@sSQLDirDefault	is	null	--при отсутствии параметра DefaultData умолчальная директория баз совпадает с системной из подключа @sInstanceKey\Setup[SQLDataRoot] с добавлением \Data
begin
	set	@sKey=	@sInstanceKey+	'\Setup'
----------
	exec	xp_instance_regread
			@rootkey=	@sHKLM
			,@key=		@sKey
			,@value_name=	'SQLDataRoot'
			,@value=	@sSQLDirDefault	out
----------
	set	@sSQLDirDefault=	@sSQLDirDefault+	'\Data'
end
----------
set	@cTemp=	cursor	local	fast_forward	for
			select
				Id
				,ServerQuoted
				,Folder
				,FileName
				,DB
				,Moment
			from
				#Body
			where
				Action=	'restore'
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
	fetch	next	from	@cTemp	into	@iStep,	@sServerQuoted,	@sFromFolder,	@sFromFile,	@sDB,	@dtMoment
	if	@@fetch_status<>	0	break
----------
	select	@sBackupDirFileQuoted=	''''+	@sFromFolder+	@sFromFile+	''''
		,@sScriptTemp=		'restore	labelonly	from	disk=	'+	@sBackupDirFileQuoted
----------
	if	@bDebug=	1
		print	( @sScriptTemp )
----------
	truncate	table	#File_LabelOnly					-- из-за наличия continue в тексте стирать лучше до
----------
	insert	#File_LabelOnly							-- лог jobа покажет ошибку на залоченном файле, который, например, ещё недоразжат архиватором
	exec	( @sScriptTemp )						-- датасет от restore нельзя подавать в insert
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
	exec	( @sScriptTemp )						-- датасет от restore нельзя подавать в insert
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
	select	top	1
		@dLSNFirst=	ISNULL ( bh.FirstLsn,	0 )
		,@dLSNLast=	ISNULL ( bh.LastLsn,	0 )
	from
		#File_BackupHeader	bh
	where
			BackupType	in	( 1,	2 )
		and	(	BackupFinishDate<=	@dtMoment		-- ***более свежие бекапы пропускаем. если быть уверенным, что бекапы отсортированы по времени, то можно прекратить обработку этой БД
			or	@dtMoment	is	null )
	if	@@RowCount<>	1						-- ***больше одного бекапа в файле не поддерживается
		update
			#Body
		set
			Message=	isnull ( Message+	'; ',	'' )+	'ошибка LSN headeronly 1'
		where
			id=	@iStep
	else
		update
			#Body
		set
			LSNFirst=	@dLSNFirst
			,LSNLast=	@dLSNLast
		where
			id=	@iStep
----------
	select
		@iRowCount=	count ( * )
	from
		sys.master_files
	where
			database_id=	db_id ( @sDB )
		and	type=		0
		and	state	in	( 0/*online*/,	1/*restoring*/  )
		and	redo_start_lsn	is	not	null
	group	by
		database_id
	having
		@dLSNFirst<=	min ( redo_start_lsn )	and	min ( redo_start_lsn )<	@dLSNLast	-- например, поданы файлы со старыми бекапами
	if	@@Error<>	0	or	@iRowCount=	0
	begin
		update
			#Body
		set
			Message=	isnull ( Message+	'; ',	'' )+	'ошибка LSN headeronly 2'
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
		--,@sDBStandby=		isnull ( sd2.name,	sd1.name )
		,@dFirstLSN=		bh.FirstLSN
		,@databaseBackupLSN=	bh.databaseBackupLSN
	from
		#File_BackupHeader	bh
		inner	join	server...sysdatabases	sd1	on
			sd1.name=	bh.databaseName
		left	join	server...sysdatabases	sd2	on
			sd2.name=	@sDBLogShippedSign+	bh.databaseName
	where
			isnull ( sd2.name,	sd1.name )=	@sDBStandby
*/

--***проверить совпадение @iBackupType и расширения файла












end
----------
deallocate	@cTemp
----------
update
	b
set
	b.Message=	isnull ( b.Message+	'; ',	'' )+	'пропускаем уже восстановленный бекап с меньшим LSN'
from
	sys.master_files	mf
	,#Body			b
where
		db_name ( mf.database_id )=	case
							when	b.Access	is	null	then	''
							else						@sDBLogShippedSign
						end+	b.DB
	and	b.LSNLast<=	isnull ( mf.redo_start_lsn,	mf.differential_base_lsn )
	and	b.Action=	'restore'
----------
update
	b1
set
	b1.Message=	isnull ( b1.Message+	'; ',	'' )+	'пропускаем при наличии более позднего полного бекапа'
from
	#Body	b1
	,#Body	b2
where
		b1.Action=	'restore'
	and	b2.Action=	b1.Action
	and	b2.Parent=	b1.Parent
	and	b2.ToDB=	b1.ToDB
	and	b2.Extension=	'bak'
	and	b1.LSNLast<	b2.LSNLast
	and	b1.Message	is	null
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
	and	( bN.Action=		b.Action	or	isnull ( bN.Action,		b.Action )		is	null )
	and	( bN.FromServer=	b.FromServer	or	isnull ( bN.FromServer,		b.FromServer )		is	null )
	and	( bN.FromDB=		b.FromDB	or	isnull ( bN.FromDB,		b.FromDB )		is	null )
	and	( bN.FromFolder=	b.FromFolder	or	isnull ( bN.FromFolder,		b.FromFolder )		is	null )
	and	( bN.FromFile=		b.FromFile	or	isnull ( bN.FromFile,		b.FromFile )		is	null )
	and	( bN.ToServer=		b.ToServer	or	isnull ( bN.ToServer,		b.ToServer )		is	null )
	and	( bN.ToDB=		b.ToDB		or	isnull ( bN.ToDB,		b.ToDB )		is	null )
	and	( bN.ToFolder=		b.ToFolder	or	isnull ( bN.ToFolder,		b.ToFolder )		is	null )
	and	( bN.ToFolderFinal=	b.ToFolderFinal	or	isnull ( bN.ToFolderFinal,	b.ToFolderFinal )	is	null )
	and	( bN.ToFile=		b.ToFile	or	isnull ( bN.ToFile,		b.ToFile )		is	null )
	and	( bN.IsCopyOnly=	b.IsCopyOnly	or	isnull ( bN.IsCopyOnly,		b.IsCopyOnly )		is	null )
	and	( bN.Moment=		b.Moment	or	isnull ( bN.Moment,		b.Moment )		is	null )
	left	join	#Body	bP	on
		bP.Parent=	b.Parent
	and	b.Id<		bP.Id	and	bP.Id<		bN.Id
	and	(	( bP.Action<>		b.Action	and	isnull ( bP.Action,		b.Action )		is	not	null )
		or	( bP.FromServer<>	b.FromServer	and	isnull ( bP.FromServer,		b.FromServer )		is	not	null )
		or	( bP.FromDB<>		b.FromDB	and	isnull ( bP.FromDB,		b.FromDB )		is	not	null )
		or	( bP.FromFolder<>	b.FromFolder	and	isnull ( bP.FromFolder,		b.FromFolder )		is	not	null )
		or	( bP.FromFile<>		b.FromFile	and	isnull ( bP.FromFile,		b.FromFile )		is	not	null )
		or	( bP.ToServer<>		b.ToServer	and	isnull ( bP.ToServer,		b.ToServer )		is	not	null )
		or	( bP.ToDB<>		b.ToDB		and	isnull ( bP.ToDB,		b.ToDB )		is	not	null )
		or	( bP.ToFolder<>		b.ToFolder	and	isnull ( bP.ToFolder,		b.ToFolder )		is	not	null )
		or	( bP.ToFolderFinal<>	b.ToFolderFinal	and	isnull ( bP.ToFolderFinal,	b.ToFolderFinal )	is	not	null )
		or	( bP.ToFile<>		b.ToFile	and	isnull ( bP.ToFile,		b.ToFile )		is	not	null )
		or	( bP.IsCopyOnly<>	b.IsCopyOnly	and	isnull ( bP.IsCopyOnly,		b.IsCopyOnly )		is	not	null )
		or	( bP.Moment<>		b.Moment	and	isnull ( bP.Moment,		b.Moment )		is	not	null ) )
where
		bP.Id	is	null
----------









--***оставить только полные Suit, например, для баз dba+dbb+dbc файлы dba1+dbb1+dbc1 образуют полный suit, а dba2+dbb2 неполный, который должен ждать появления dbc2 для своей полноты. неполные наборы поместить в список warning



--***внутри каждого Suit базы отсортировать подряд для минимизации файловых операций















if	@bDebug=	1
begin
	select * from #BackupDir
	select * from #Body
end




----------
set	@cSuit=	cursor	local	fast_forward	for
			select
				Id
				,Suit
				,Server
				,quotename ( Server )
				,isnull ( IsAsyncronous,	0 )
				,ProcName
			from
				#Suit
			order	by
				Id
----------
open	@cSuit
----------
while	1=	1
begin
	fetch	next	from	@cSuit	into	@iParent,	@sSuit,	@sServer,	@sServerQuoted,	@bIsAsyncronous,	@sProcName
	if	@@fetch_status<>	0	break
----------
	if	@sServer<>	@@ServerName
	begin
/*		select	@sScriptTemp=	( select
						[data()]=	text								-- название колонки должно отсутствовать для работы функции text()
					from
						syscomments
					where
							id=		@@procid
						and	number<>	0
						and	encrypted=	0
					for
						xml	path ( '' )								-- не годится, т.к. между строками добавляет пробел
						,TYPE ).value ( '(./text())[1]',	'nvarchar(max)' )			-- чтобы не заменять xml спецсимволы
*/
		set	@sScriptTemp=	''
----------
		select
			@sScriptTemp=	@sScriptTemp+	text
		from
			syscomments
		where
				id=		@@procid
			and	number<>	0
			and	encrypted=	0
		order	by
			colid
----------
		select	@sProcName=	quotename ( isnull ( object_name ( @@procid )+	convert ( varchar ( 36 ),	newid() )
										,	( select
												name
											from
												sysobjects			-- учесть, что object_name ищет временные процедуры в текущей базе, а не tempdb, т.е. не работает
											where
													id=	@@procid
												and	xtype=	'p' ) ) )
			,@sScript=	'create	proc	'+	/*schema_name ( objectproperty ( @@procid,	'OwnerId' ) )*/+	@sProcName+	'
	'+	right ( @sScriptTemp,	len ( @sScriptTemp )-	patindex ( '%[^a-z0-9]@[a-z0-9]%as%',	@sScriptTemp ) )	-- считаем, что параметр у процедуры есть; заменяем имя процедуры, но схему не указываем, пусть процедура будет под пользователем linked сервера
----------
		set	@xPart=	( select
					s.Suit
					,s.IsAsyncronous
					,s.Action
					,s.FromServer
					,s.FromDB
					,s.FromFolder
					,s.FromFile
					,s.ToServer
					,s.ToDB
					,s.ToFolder
					,s.ToFolderFinal
					,s.ToFile
					,s.IsCompressed
					,s.IsCopyOnly
					,s.Moment

					,b.Action
					,b.FromServer
					,b.FromDB
					,b.FromFolder
					,b.FromFile
					,b.ToServer
					,b.ToDB
					,b.ToFolder
					,b.ToFolderFinal
					,b.ToFile
					,b.IsCompressed
					,b.IsCopyOnly
					,b.Moment
					,b.Access
				from
					#suit	s
					,#body	b
				where
						s.Id=		@iParent
					and	b.Parent=	s.Id
					and	b.Message	is	null	-- как отличить ошибку от предупреждения?
				order	by
					b.Id
				for
					xml	auto
					,root ( 'PlanBR' ) )
	end
	else
	begin
		select	@sProcName=	quotename (	case
								when	@sServer	is	null	then	'##'	-- только на локальном сервере временный процедуры доживут от создания до выполнения
								else						''
							end
						+	@sNow1
						+	'_PlanBR_'
						+	replace ( @@servername,	'\',	'!' )
						+	@sDBLogShippedSign )	-- ! для единообразия с именем файла
			,@sScript=	'
create	proc	'+	@sProcName+	'
	@sDBList	varchar ( 8000 )	output
as
set	nocount	on
----------
drop	proc	'+	@sProcName+	'	-- важно для постоянных процедур, лучше сделать в конце, чтобы текст процедуры можно было получить до её конца выполнения, но тогда есть шанс вывалиться по ошибке и не дойти до конца
----------
declare	@iFSO		int
	,@iError	int

	,@sExec		varchar ( max )
	,@iDB		integer
----------
set	@sDBList=	null'
----------

--***передавать в процедуру BodyId





----------
		set	@cBodySteps=	cursor	local	forward_only	for			-- курсор переопределяем каждую итерацию, недостаточно переоткрывать, т.к. он использует переменную @iParent
						with	cteBody	as
						(	select
								*
								,Sequence2=	row_number()	over	( partition	by	Parent	order	by	Id )
							from
								#Body
							where
									Parent=	@iParent
								and	Message	is	null )	-- если там только ошибки, а не предупреждения
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
							,b.ToFolderFinal
							,b.ToFile
							,b.IsCompressed
							,b.IsCopyOnly
							,b.Access
							,b.Server
							,b.ServerQuoted
							,b.DB
							,b.DBQuoted
							,b.Extension
							,b.LSNFirst
							,b.LSNLast
							,DBLive=		b.DB
							,DBStandBy=		case
											when	b.Access	is	null	then	''
											else						@sDBLogShippedSign
										end
									+	b.DB
							,AfterDBChange=		case
											when		bP.Action<>	b.Action
												or	isnull ( bP.Server,	'' )<>	isnull ( b.Server,	'' )
												or	bP.DB<>		b.DB
												or	bP.Id	is	null	then	1
											else						0
										end
							,BeforeDBChange=	case
											when		bN.Action<>	b.Action
												or	isnull ( bN.Server,	'' )<>	isnull ( b.Server,	'' )
												or	bN.DB<>		b.DB
												or	bN.Id	is	null	then	1
											else						0
										end
							,AfterServerChange=	case
											when		bP.Action<>	b.Action
												or	isnull ( bP.Server,	'' )<>	isnull ( b.Server,	'' )
												or	bP.Id	is	null	then	1
											else						0
										end
							,BeforeServerChange=	case
											when		bN.Action<>	b.Action
												or	isnull ( bN.Server,	'' )<>	isnull ( b.Server,	'' )
												or	bN.Id	is	null	then	1
											else						0
										end
						from
							cteBody	b
							left	join	cteBody	bP	on
								bP.Sequence2=	b.Sequence2-	1
							left	join	cteBody	bN	on
								bN.Sequence2=	b.Sequence2+	1
						order	by
							b.Id
----------
		open	@cBodySteps
----------
		while	1=	1
		begin
			fetch	next	from	@cBodySteps	into	@iStep,	@sAction,	@sFromServer,	@sFromDB,	@sFromFolder,	@sFromFile,	@sToServer,	@sToDB,	@sToFolder,	@sToFolderFinal,	@sToFile,	@bIsCompressed,	@bIsCopyOnly,	@sAccess,	@sServer,	@sServerQuoted,	@sDB,	@sDBQuoted,	@sExtension,	@dLSNFirst,	@dLSNLast,	@sDBLive,	@sDBStandBy,	@bAfterDBChange,	@bBeforeDBChange,	@bAfterServerChange,	@bBeforeServerChange
			if	@@fetch_status<>	0	break
----------
			set	@sScriptPart=	''
----------
			if	@sAction=	'restore'
			begin
				if	exists	( select
							1
						from
							sys.master_files
						where
								db_name ( database_id )=	@sDBStandby
							and	@dLSNLast<	isnull ( redo_start_lsn,	differential_base_lsn ) )
				begin
					update
						#Body
					set
						Message=	isnull ( Message,	'' )+	' LSN бекапа младше LSN базы или неверно задана база '+	@sDBStandby
					where
						Id=		@iStep
				end
				else
				begin
					/*select
						@iDBCount=	count (	distinct	DB )
					from
						#Body
					where
						Parent=		@iParent*/
----------
					select
						@sScriptPart=	@sScriptPart+	'
----------
EXEC	sp_OACreate	''Scripting.FileSystemObject'',	@iFSO	OUT'
					where
						@bAfterServerChange=	1
----------

/*					if	@bIsCoupled=	1
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




--@sDBStandby	довосстанавливаем(передавая исходное имя), копируем в @sDBLive	с гуидным именем
--@sDBLive	блокируем для восстановления, читаем, пишем			с исходным именем




--					if	@@RowCount=	1						-- правильно распознали название базы в имени файла, например, 'DB' и 'DB1', здесь 1- часть названия базы или разделитель в имени файла?
					begin
						set	@sLockName=	upper ( @sDBLive )+	@sProjectSign	-- сравнивается в бинарном виде, а одна и та же база может на разных серверах иметь имя в разном регистре
----------
						select
							@sScriptPart=	@sScriptPart+	'
----------
exec	@iError=	master..sp_getapplock					-- блокируем ресурс; если нужно запретить копирование в активированную базу, то блокировку нужно запросить с @LockMode=Exclusive, например, при выполнении длительного отчёта
				@Resource=	'''+	@sLockName+	'''	-- делаем все блокировки относительно master упрощая работу, т.к. блокировки учитываются относительно текущей базы, поэтому и проверять их нужно относительно заданной базы, базу можно задать в самом execute
				,@LockMode=	''Shared''
				,@LockOwner=	''Session''
				,@LockTimeout=	0
if	@@Error<>	0	or	@iError<>	0
begin
	set	@sDBList=	isnull ( @sDBList,	'''' )+	'''+	@sDBLive+	@sDBDelimeter+	'''
	goto	skip_'+	upper ( @sDBLive )+	'
end'+	case
		when	db_id ( @sDBStandby )	is	null	then	''
		else	'
----------
set	@iDB=	'+	convert ( varchar ( 10 ),	db_id ( @sDBStandby ) )+	'
----------
while	1=	1
begin
	set	@sExec=	''''
----------
	select
		@sExec=	@sExec+	'';kill	''+	convert ( varchar ( 10 ),	req_spid )
	from
		( select	distinct
			req_spid
		from
			syslockinfo
		where
			rsc_dbid=		@iDB
		union
		select
			spid
		from
			sysprocesses
		where
				dbid=		@iDB
			and	hostprocess=	'''' )	t
----------
	if	@sExec<>	''''
		begin	try
			exec	( @sExec )	-- может отключить непричастных, если логины часты
		end	try
		begin	catch
		end	catch
	else
		break
end'
	end
						where
							@bAfterDBChange=	1
----------
						set	@bRestoring=	1

/*


						set	@bRestoring=	@bRestoring&	case
												when	exists	( select
															1
														from
															#BackupDirParsed	d1
															,#BackupDirParsed	d2
														where
																d1.FileName=	@sBackupFile
															and	d2.Extension=	@sExtBak		-- если есть последующие .bak файлы этой базы, то промежуточные .trn и .bak восстанавливать не нужно
															and	left ( d2.FileName,	patindex ( '%'+	d2.Cutoff+	'%',	d2.FileName )-	1 )=	@sDBLive+	@sBackupSign
															and	d1.Cutoff<	d2.Cutoff	 )	then	0
												else										1
											end
*/
----------
						--if	@bRestoring=	1
						begin
							select	@sBackupDirFileQuoted=	''''+	@sFromFolder+	@sFromFile+	''''
								,@sScriptTemp=		'restore	filelistonly	from	disk=	'+	@sBackupDirFileQuoted+	'	with	nounload'
----------
							if	@bDebug=	1
								print	( @sScriptTemp )
----------
							truncate	table	#File_FileListOnly
----------
							insert	#File_FileListOnly
							exec	( @sScriptTemp )
							if	@@error<>	0	continue	-- битый список файлов? нужно бы обрабатывать ошибки
----------
					-- ***этот запрос нужно выполнить на той стороне?
							select
								@sSQLDir=	left ( filename,	len ( filename )-	charindex ( '\',	reverse ( filename ) ) )+	'\'
							from
								sysdatabases
							where
								name=	@sDB
----------
							if		@@RowCount<>	1
								or	isnull ( @sSQLDir,	'' )=	''
								set	@sSQLDir=	@sSQLDirDefault
----------
							set	@sScriptTemp=	'
select
	sdb.dbid
	,fileid=	saf.file_id
	,sdb.name
	,LogicalName=	saf.name
	,filename=	saf.physical_name
	,TempName=	replace ( convert ( varchar ( 36 ),	newid() ),	''-'',	'''' )	-- получаем гуид на той стороне
from
	sysdatabases		sdb
	,sys.master_files	saf
where
		sdb.name	like	''%'+	@sDB+	'%''
	and	saf.database_id=sdb.dbid'
----------
							truncate	table	#Server_DBFiles
----------
							insert	#Server_DBFiles	( dbid,	fileid,	DBName,	LogicalName,	filename,	TempName )
							exec	( @sScriptTemp )
----------
							truncate	table	#Generation_Files
----------
							insert
								#Generation_Files	( DBStandby,	DBLive,	StandbyFileName,	LiveFileName,	TempFileName,	IsDBStandbyCreate )
							select
								case
									when	@sAccess	is	not	null	then	isnull ( stby.DBName,	@sDBLogShippedSign+	live.DBName )
									else							live.DBName
								end
								,live.DBName
								,case
									when	@sAccess	is	not	null	then	isnull ( stby.filename,	live.Path+	@sDBLogShippedSign+	right ( live.filename,	charindex ( '\',	reverse ( live.filename ) )-	1 ) )
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
							if	@sExtension=	@sExtBak
							begin
								/*if	@bDebug=	1
								begin
									select * from #Generation_Files
									select * from #File_FileListOnly
									select * from #Server_DBFiles
								end*/
----------
								set	@sScriptPart=	@sScriptPart+	'
----------
restore	database
	'+	@sDBStandby+	'
from
	disk=	'+	@sBackupDirFileQuoted+	'
with
	standby=	'''+	@sFromFolder+	@sDBStandby+	'.TUF''
	,nounload
	,replace
	,stats=	100'
----------
								set	@sUnique=	replace ( convert ( varchar ( 36 ),	newid() ),	'-',	'' )
----------
								select
									@sScriptPart=	@sScriptPart+	'
	,move	'''
										+	flo.LogicalName
										+	'''	to	'''
										+	coalesce ( @sToFolder,	saf.Path,	@sSQLDir )	-- считаем заданную директорию важнее, чем та, где уже лежат файлы
										+	case	@sDBLive
												when	@sDBStandby	then	''
												else				@sDBLogShippedSign
											end
										+	isnull ( saf.File1,	right ( flo.PhysicalName,	charindex ( '\',	reverse ( flo.PhysicalName ) )-	1 ) )	-- считаем имена уже существующих файлов важнее, т.к. они могут быть переименованы согласно местным условиям
										+	case
												when	saf2.filename	is	null	then	''
												else					@sUnique	-- уникализируем имя файла, например, если возстановлено несколько тестовых баз
											end
										+	''''					-- вырезать имя файла из полного пути
								from
									#File_FileListOnly	flo
									left	join	#Server_DBFiles	saf	on
										saf.LogicalName=	flo.LogicalName
									and	saf.DBName=		@sDB
									left	join	#Server_DBFiles	saf2	on
										saf2.filename=		isnull ( saf.Path,	@sSQLDir )	-- база может восстанавливаться под другим названием
												+	case	@sDBLive
														when	@sDBStandby	then	''
														else				@sDBLogShippedSign
													end
												+	right ( flo.PhysicalName,	charindex ( '\',	reverse ( flo.PhysicalName ) )-	1 )





								/*select
									flo.LogicalName
									,saf.filename
									,@sSQLDir
									,@sDBLive
									,@sDBStandby
									,@sDBLogShippedSign
									,flo.PhysicalName
								from
									#File_FileListOnly	flo
									left	join	#Server_DBFiles	saf	on
										saf.LogicalName=	flo.LogicalName
									and	saf.DBName=		@sDB*/

--select * from #Server_DBFiles








							end
							else
								if	@sExtension=	@sExtTrn
									set	@sScriptPart=	@sScriptPart+	'
----------
restore	log
	'+	@sDBStandby+	'
from
	disk=	'+	@sBackupDirFileQuoted+	'
with
	file=		1
	,standby=	'''+	@sFromFolder+	@sDBStandby+	'.TUF'''
						end
----------
						set	@sScriptPart=	@sScriptPart+	'
----------
'/*+	case	@bRestoring		-- бекапы не стираем, чтобы можно было восстановить тестовые базы с другим именем; о стирании файлов заботится внешний процесс, чтобы эта процедура не тратила время на уже неактуальные файлы бекапов
		when	1	then	'if	@@Error=	0
	'
		else			''
	end+	'exec	sp_OAMethod	@iFSO,	''DeleteFile'',	null,	'+	@sBackupDirFileQuoted+	',	1'*/
----------
						select
							@sScriptPart=	@sScriptPart+	'
----------
alter	database	'+	@sDBStandby+	'	set	offline'
						where
							@sAccess	is	not	null
----------
						select
							@sScriptPart=	@sScriptPart+	'
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
							@sScriptPart=	@sScriptPart+	'
----------
alter	database	'+	@sDBStandby+	'	set	online
----------
alter	database	'+	@sDBLive+	'	set	offline'
						where
							@sAccess	is	not	null
----------
						select
							@sScriptPart=	@sScriptPart+	'
----------
exec	sp_OAMethod	@iFSO,	''DeleteFile'',	'''+	@sSQLDir+	LiveFile+	''''	--переименовываем скопированные под другим именем файлы в файлы Live базы
						from
							#Generation_Files
						where
							@sAccess	is	not	null
----------
						select
							@sScriptPart=	@sScriptPart+	'
----------
exec	sp_OAMethod	@iFSO,	''MoveFile'',	'''+	@sSQLDir+	TempFileName+	''',	'''+	@sSQLDir+	LiveFile+	''''	--переименовываем скопированные под другим именем файлы в файлы Live базы
						from
							#Generation_Files
						where
							@sAccess	is	not	null
----------
						select
							@sScriptPart=	@sScriptPart+	'
----------
alter	database	'+	@sDBLive+		'	set	online
----------
exec	'+	@sDBLive+	'..sp_changedbowner	'''+	'dbo'/*@sOwner*/+	''''	-- базе в standby нельзя сделать sp_changedbowner
						where
							@sAccess	is	not	null
					end
----------
					select
						@sScriptPart=	@sScriptPart+	'

skip_'+	upper ( @sDBLive )+	':

exec	@iError=	master..sp_releaseapplock		-- отпускаем блокировку
				@Resource=	'''+	@sLockName+	'''
				,@LockOwner=	''Session'''
					where
						@bBeforeDBChange=	1
----------
					select
						@sScriptPart=	@sScriptPart+	'
----------
exec	sp_OADestroy	@iFSO'
					where
						@bBeforeServerChange=	1
				end
			end
----------------------------------------------------------------------------------------------------
			else
				if		@sAction	like	'backup database'
					or	@sAction	like	'backup log'
				begin
					select	@sCutoff=		str ( year ( @dtNow ),	4 )
								+	'_'
								+	replace ( str ( month ( @dtNow ),	2 ),	' ',	'0' )
								+	'_'
								+	replace ( str ( day ( @dtNow ),		2 ),	' ',	'0' )
								+	'_'
								+	replace ( replace ( right ( @sNow,	12 ),	':',	'' ),	'.',	'_' )
								--+	'0000'
						,@sBackupInfo=		@sCutoff
								+	'|'
								+	convert ( varchar ( 128 ),	convert ( varchar ( 16 ),	@sServerVersion )
								+	'|'
								+	convert ( varchar ( 8 ),	SERVERPROPERTY ( 'EngineEdition' ) )
								+	'|'
								+	@@servername+	isnull ( '.'+	db_name()+	'.'+	schema_name ( OBJECTPROPERTY ( @@procid,	'OwnerId' ) )+	'.'+	object_name ( @@procid ),	'' )
								+	'|'
								+	convert ( varchar ( 2 ),	@@NESTLEVEL )
								+	isnull ( '|'+	app_name(),	'' ) )
						,@sBackupInfoStr=	replace ( @sBackupInfo,	'''',	'''''' )	-- чтобы при склеивании команды символ сохранился
----------
					set	@sScriptPart=		'
----------
select							-- считаем, что нельзя одновременно бекапить несколько раз, хотя сервер и сам останавливается и ждёт завершения бекапа в другом соединении
	@sDBList=	isnull ( @sDBList,	'''' )+	DB_NAME ( database_id )+	'''+	@sDBDelimeter+	'''
from
	master.sys.dm_exec_requests			-- sql 2005+
where
		command	like	''BACKUP%''
	and	DB_NAME ( database_id )=	'''+	@sDB+	'''
if	@@Rowcount=	0
begin
	BACKUP	'
								+	case
										when	@sAction	like	'%database'	then	'database'
										else							'log'
									end
								+	'
		'+	@sDBQuoted+	'
	TO
		DISK=	'''+	@sToFolder+	@sToFile+	'''
	WITH
		FORMAT
		,INIT
		,NAME=	'''+	@sBackupInfoStr+	'''
		,SKIP
		,STATS=	100'
								+	case	@bIsCompressed
										when	1	then	'
		,COMPRESSION'
										when	0	then	'
		,NO_COMPRESSION'
										else			''					-- из серверных настроек по умолчанию
									end
								+	case	@bIsCopyOnly
										when	1	then	'
		,COPY_ONLY'
										else			''
									end
							+	'
	if	@@Error<>	0
		set	@sDBList=	isnull ( @sDBList,	'''' )+	'''+	@sDB+	@sDBDelimeter+	'''
end'
				end
----------
			if	isnull ( @sScriptPart,	'' )<>	''
				select	@sScript=	@sScript+	@sScriptPart
		end
----------
		deallocate	@cBodySteps
	end
----------
	if	@bDebug=	1
	begin
		print	substring(@sScript,1,4000)
		print	substring(@sScript,4001,8000)
		print	substring(@sScript,8001,12000)
		print	substring(@sScript,12001,16000)
		print	substring(@sScript,16001,20000)
		print	substring(@sScript,20001,24000)
		print	substring(@sScript,24001,28000)
		print	substring(@sScript,28001,32000)
		print	substring(@sScript,32001,36000)
		print	substring(@sScript,36001,40000)
		print	substring(@sScript,40001,44000)
		print	substring(@sScript,44001,48000)
		print	substring(@sScript,48001,52000)
		print	substring(@sScript,52001,56000)
		print	substring(@sScript,56001,60000)
		print	substring(@sScript,60001,64000)
		print	substring(@sScript,64001,68000)
		print	substring(@sScript,68001,72000)
	end
----------
	if	@sServer<>	@@ServerName
	begin
		if	@xPart	is	not	null
		begin
			select	@sXML=		convert ( varchar ( max ),	@xPart )
				,@sScript=	'
exec ( '''+	replace ( @sScript,	'''',	'''''' )+	'
'')
exec	'+	@sProcName+	'
		@tPlan=		@x
		,@tMessage=	@s	output'
				,@sExecAtServer=	@sServerQuoted+	'.tempdb..sp_executesql'	-- чтобы процедура без ## на другом сервере создавалась в tempdb, а не в базе по умолчанию для логина линкед сервера
----------
			exec	@sExecAtServer
					@statement=	@sScript
					,@params=	N'@x	text
							,@s	varchar ( 8000 )	out'
					,@x=		@sXML
					,@s=		@sDBList	out
		end
	end
	else
		exec	( @sScript )				-- сначала создать все процедуры, потом выполнять их, возможно асинхронно
----------
	update
		#Suit
	set
		ProcName=	@sProcName
	where
		Id=		@iParent
----------
	set	@sDBListOut=	@sDBListOut+	isnull ( @sDBListOut2+	@sDBDelimeter,	'' )
end
----------
close	@cSuit
----------
EXEC	@iError=	sp_OACreate
				'ADODB.Connection'
				,@iOLEConnection	OUTPUT
IF	@@Error<>	0	or	@iError<>	0
BEGIN
	EXEC	sp_OAGetErrorInfo	@iOLEConnection,	@sMessage1	OUTPUT,	@sMessage	OUTPUT
	set	@sMessage=	@sMessage1+	@sMessage
	goto	error
END
----------
open	@cSuit
----------
while	1=	1
begin
	fetch	next	from	@cSuit	into	@iParent,	@sSuit,	@sServer,	@sServerQuoted,	@bIsAsyncronous,	@sProcName
	if	@@fetch_status<>	0	break
----------
	if	@bIsAsyncronous=	0
		exec	@sProcName
				@sDBList=	@sDBListOut2	output
	else
	begin


-- ***нужен параметр- не запускать параллельный процесс, если уже запущено и не завершилось по числу рарешённых ядер в affinity

		select	@sLogin=	null
			,@sPwd=		null			-- не поддерживается
			,@sConnection=	'Provider=SQLOLEDB'	-- этот провайдер всегда присутствует?
				+	';Server='+		isnull ( @sServer,	@@servername )
				+	';Database=master'
				+	';Trusted_Connection='+	case
									when	@sLogin	is	null	then	'yes'
									else					'no'
								end
----------
		EXEC	@iError=	sp_OAMethod
						@iOLEConnection
						,'Open'
						,NULL
						,@sConnection
						,@sLogin
						,@sPwd
						,-1			--adConnectUnspecified
		IF	@@Error<>	0	or	@iError<>	0
		BEGIN
			EXEC	sp_OAGetErrorInfo	@iOLEConnection,	@sMessage1	OUTPUT,	@sMessage	OUTPUT
			set	@sMessage=	@sMessage1+	@sMessage
			goto	error
		END
----------
		EXEC	@iError=	sp_OAMethod
						@iOLEConnection
						,'Execute'
						,@iDumb		output	-- при любом типе данных получим ошибку несовпадения типа 0x80020005, которую нужно проигнорировать из-за того, что получить тип Recordset нет возможности
						,@sProcName
						,null
						,145			--CommandTypeEnum.adCmdText=1|ExecuteOptionEnum.adAsyncExecute=16|ExecuteOptionEnum.adExecuteNoRecords=128
		IF	@@Error<>	0	or	@iError	not	in ( 0,	0x80020005 )
		BEGIN
			EXEC	sp_OAGetErrorInfo	@iOLEConnection,	@sMessage1	OUTPUT,	@sMessage	OUTPUT
			set	@sMessage=	@sMessage1+	@sMessage
			goto	error
		END
----------
		EXEC	@iError=	sp_OAMethod
						@iOLEConnection
						,'Close'
		IF	@@Error<>	0	or	@iError<>	0
		BEGIN
			EXEC	sp_OAGetErrorInfo	@iOLEConnection,	@sMessage1	OUTPUT,	@sMessage	OUTPUT
			set	@sMessage=	@sMessage1+	@sMessage
			goto	error
		END
	end
end
----------
deallocate	@cSuit
----------
EXEC	@iError=	sp_OADestroy	@iOLEConnection


----------
goto	done

error:

if	@sMessage	is	null	set	@sMessage=	'Ошибка ручного log shipping'
raiserror ( @sMessage,	18,	1 )
--EXEC	@iError=	sp_OAGetErrorInfo	null,	@source	OUT,	@desc	OUT
--SELECT	OLEObject=	CONVERT ( binary ( 4 ),	@iError ),	source=	@source,	description=	@desc

done:

set	@sDBList=	@sDBListOut

----------
--if	cursor_status ( 'variable',	'@cTemp' )<>	-2	deallocate	@cTemp

--drop	table
--	#BackupDir