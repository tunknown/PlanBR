--(c) LGPL

use	master
go
if	object_id ( 'dbo.BackupRestore' , 'p' )	is	null
	exec	( 'create	proc	dbo.BackupRestore	as	select	ObjectNotCreated=	1/0' )
go
alter	proc	dbo.BackupRestore	-- ����� � �������������� ��� ������
	@tPlan		text			-- maintenance ���� � XML
	,@tMessage	text=	null	output	-- ������ ��������� � XML, ��������, ������ � ��, ��� �� ���� � �����, �� ��� �� ������������

--��������� �� ���� ��������� ��� ������������� �� � ����� ����� ���������?

/*
	@sAction		varchar ( 32 )
					-- ������� BACKUP DATABASE/BACKUP LOG/restore
	,@sDBList		varchar ( 8000 )	output
					-- ������ ����� ";" ��� ������(������� linked ������ ����� ������ ���� ����� ".") �� 100 �������� (=MAXRECURSION default)
					-- output=������ �������������� ��� ��� ��������� ������
					-- (		restore	)null=�������� ��������������� ��� ���� �������� �������, ��� ���� � output ��� null � �������� ������ ���������������� ���
					-- (		restore	)DB_1=DB, ������������ ���� DB ��� ������ DB_1, ��������, ��� ��������� ������� � ����������� �������� ���
	,@sBackupDir		varchar ( 260 )
					-- ������� ��� ������ *.bak,*.trn ��������� �� ���� �� ���� �� �������, ��� ����������� ������ ���� ������; ���� �������� � ������ ������� ����� ��������; ��� restore �� ������ ��������� ������, ����������/������������ ��� �� ��������������� ��� ��������������; ��� ����� � �������� ����� ���� ������ ���� ������������� �� ASC

***bit->varchar(256) ��� backup(��������, � ���������), ��� restore ������������ ����������, ������� ������ �� �������- ������ ��� ���
***@bIsCoupled->Cutoff


***��� ������� � �������� ����
***�������������� ���� ��� ������ ������ ��� ��������� �������
	,@bIsCoupled		bit
					-- ( backup	restore	)backup=��������� ���� ��������� ���������;restore=���� ������������ ������, ���� ����� �� �������, �� �� �����������; �������� � ������ ������ ������ ������������
	,@sSQLDir		varchar ( 256 )
					-- (		restore	)������� � �������(mdf/ndf/ldf) ��� ������ MSSQL
	,@sOwner		varchar ( 256 )=	null
					-- (		restore	)�������� ���� ������� ���� ���� ����������������� ��� logshipping ��� readonly
	,@sDestinationDir	varchar ( 260 )
					-- ( backup		)� ���� ������� ����� ������ ������ ���� ���������� ����� ���������� �������, �.�. ��� ����� ��� �� �������� � �� ����� ��������������; ������� ������ ���� �� ��� �� �����, ����� ��� �������������� � ���� ���� ����� ���� ���������, ���� ������� �� ������ ����� ��� �������, �� ��� ���������� ��������
	,@bIsCompressed		bit
					-- ( backup		)������������ ������ sqlserver, ���� �������, ��� ���������; ������� ������ ����� ��� �������� �� ��������� ����, ����� ������� �� ������ ��������� ���������� ��������
	,@bIsCopyOnly		bit
					-- ( backup		)������ �����, �� ���������� ������� backup log
*/
as
set	nocount	on
declare	@iError			int
	,@iRowCount		int
	,@sMessage		varchar ( 256 )
	,@bDebug		bit	-- 1=�������� ���������� ���������

	,@sBackupFile		varchar ( 256 )
	,@sBackupDirFileQuoted	varchar ( 256 )

	,@iBackupType		int
	,@sDBStandby		varchar ( 256 )
	,@sDBLive		varchar ( 256 )
	,@sCutoff		varchar ( 256 )

	,@dFirstLSN		numeric ( 25 , 0 )
	,@databaseBackupLSN	numeric ( 25 , 0 )

	,@sScript		varchar ( max )			-- �� ���������� � sql 2000
	,@sScriptTemp		varchar ( max )

	,@cTemp			cursor
	,@cDatabases		cursor
	,@cServers		cursor
	,@cSuit			cursor
	,@cBodySteps		cursor

	,@bRestoring		bit
	,@bStopWaiting		bit
	,@bQuorum		bit
	,@bAfterChange		bit
	,@bBeforeChange		bit

	,@sBackupSign		varchar ( 256 )
	,@sPattern		varchar ( 256 )
	,@sExtBak		varchar ( 256 )
	,@sExtTrn		varchar ( 256 )
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

	,@iDBCount		int		-- ����� ��� � backup, ����������������� ���������
	,@dtMoment		datetime
	,@sDBDelimeter		varchar ( 2 )

	,@sFileNameBak		varchar ( 256 )

	,@iXML			integer
	,@iParent		smallint

	,@sBackupDir		sysname
	,@sSQLDir		sysname
	,@sDBList		varchar ( 8000 )
	,@bIsCoupled		bit
	,@sOwner		sysname

	,@bIsAsyncronous	bit





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

----------
select	@bDebug=		1
	,@sExtBak=		'bak'							-- ������ ���� ���������� � MainenancePlan
	,@sExtTrn=		'trn'
	,@sProjectSign=		'363B1BEF8FA34873824C29D1EBC10C79'
	,@sDBLogShippedSign=	'z'+	@sProjectSign					-- �������, ��� ���� ��� log shipping ����� ���������� ������� � �����
	,@sDBDelimeter=		';'
	,@sDBListOut=		''
----------
create	table	#Suit	-- ������ ������
-- ���� ������ ���������� � ���������� ������; ���� � ���������� ������ ����, �������� �� ���������, �� ������
(	Id		smallint	not null	unique	clustered	--backup/restore		������������� ������������� � OPENXML, ������������ ��� ����������
	,Suit		sysname		null		--backup/restore		-- ""=�������������� ����� �����������; ������� ����� ��� ��������� ������, �������� ��� ������ ��� ����� Cutoff � ��������������� ��� ������ � ����� ����� Cutoff, ���� �� ������� �����, �� �� ��������������� �� ������
	,IsAsyncronous	bit		null		--backup/restore
	,Server		sysname		null		--�� ���� linked ������� ����������� ��������� ���� ������
--����� ���� ��� �������� ��������� ���������� � #Body
	,Action		varchar ( 256 )	null		-- backup database/backup log/restore
	,FromServer	sysname		null		--backup/restore
	,FromDB		sysname		null		--backup/restore
	,FromFolder	sysname		null		--restore
	,FromFile	sysname		null		--restore		-- �������� ��� ���� � ������� � ����� ����� �� ������ ������ � ���� ������� ������������ ��� CutOff
	,ToServer	sysname		null		--restore
	,ToDB		sysname		null		--restore
	,ToFolder	sysname		null		--backup/restore
	,ToFile		sysname		null		--backup
	,IsCompressed	bit		null		--backup
	,IsCopyOnly	bit		null		--backup
,check	( Action	in	( 'backup database' , 'backup log' , 'restore' ) )	)
----------
create	table	#Body	-- ���������� ������
(	Id		smallint	not null	unique	clustered	--backup/restore
	,Parent		smallint	not null	foreign	key	references	#Suit ( Id ) --backup/restore, ��� �����������, �� �����- FK �� ��������

	,Action		varchar ( 256 )	null		-- backup database/backup log/restore; ��� backup ��������� �������� �� FromServer, ��� restore ��������� �������� �� ToServer, � �������� ������ Suit ��� ���� ������ ���� ��������� ���������
	,FromServer	sysname		null		--backup/restore		��� ������� �� ������ ������, ��������, ���� ���� � ����� ������ ���������� �� 2� ��������
	,FromDB		sysname		null		--backup/restore
	,FromFolder	sysname		null		--restore
	,FromFile	sysname		null		--restore		-- �������� ��� ���� � ������� � ����� ����� �� ������ ������ � ���� ������� ������������ ��� CutOff
	,ToServer	sysname		null		--restore
	,ToDB		sysname		null		--restore
	,ToFolder	sysname		null		--backup/restore
	,ToFile		sysname		null		--backup
	,IsCompressed	bit		null		--backup
	,IsCopyOnly	bit		null		--backup
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
,unique	( Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFile,	IsCompressed,	IsCopyOnly )
,check	( Action	in	( 'backup database' , 'backup log' , 'restore' ) )
--,check	( FromDB	is	not	null	or	ToDB		is	not	null )	--/����������� �� �������� ��� �������������� ����������; ���� ������������ ��� ��������� ����, �� �������� �� �����������
--,check	( FromFolder	is	not	null	or	ToFolder	is	not	null )	--\
 )
----------
if	isnull ( datalength ( @tPlan ) , 0 )<	4
begin
	select	@sMessage=	'���� �������������� � �������� �������',
		@iError=	-3
	goto	error
end
----------
EXEC	@iError=	sp_xml_preparedocument	@iXML	OUTPUT,	@tPlan		-- sql2000+
if	@@Error<>	0	or	@iError<>	0
begin
	select	@sMessage=	'������ XML 1',
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
	OPENXML	( @iXML,	'/PlanBR/s',	1 )
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
	#Body	( Id,	Parent,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFile,	IsCompressed,	IsCopyOnly )
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
from
	OPENXML	( @iXML,	'/PlanBR/s/b',	1 )
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
	,IsCopyOnly	bit )
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
	set	@sMessage=	'������ ������ ���������� � ���������: '+	@sMessage
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
	set	@sMessage=	'������ ������ ���������� � ���������: ����� ������ ������ ������ ����� ������'
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
create	table	#BackupHeader	-- ����� �� ��������� master.dbo.sp_can_tlog_be_applied
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
if		convert ( varchar ( 10 ) , serverproperty ( 'ProductVersion' ) )	like	'%.%'	-- ��������� ������ �������
	and	10<	left ( convert ( varchar ( 10 ) , serverproperty ( 'ProductVersion' ) ) , charindex ( '.' , convert ( varchar ( 256 ) , serverproperty ( 'ProductVersion' ) ) ) - 1 )
begin
	alter	table	#LabelOnly	add
		MirrorCount		int	-- � ������������ ���� ���������� Mirror_Count
		,IsCompressed		bit
----------
	alter	table	#BackupHeader	add
		CompressedBackupSize	bigint
		,containment		tinyint
----------
	alter	table	#FileListOnly	add
		TDEThumbprint		varbinary ( 32 )
----------
	set	@sPattern=		'%2[0-9][0-9][0-9][_][0-1][0-9][_][0-3][0-9][_][0-2][0-9][0-5][0-9][0-5][0-9][_][0-9][0-9][0-9][0-9][0-9][0-9][0-9].%'	-- ����� ����� backup ����� �� MaintenancePlan sql 2012
end
else
begin
	alter	table	#LabelOnly	add
		Mirror_Count		int
----------
	set	@sPattern=		'%2[0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9].%'								-- ����� ����� backup ����� �� MaintenancePlan sql 2005
end











----------
/*if	exists	( select
			1
		from
			( select
				Sequence=	DENSE_RANK()	over	( partition	by	Parent	order	by	DB )
			from
				#Body )	�
		where
			Sequence<>	1 )
begin
	set	@sMessage=	'������ ������ ���������� � ���������: ���� ������ ���� ����� �� ���� ������, ���� ��������� �� ���� ������'
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
				when	b.Action	like	'backup%'	then	replace ( isnull ( b.Server , @@servername ) , '\' , '!' )	-- ***�� ����� ����� ������ ������, ���� ���� � linked server ��� � ���������� ������� � ����� �� ������
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
				else							''
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


select * from #Suit
select * from #Body






----------
set	@cSuit=	cursor	local	fast_forward	for
			select
				Id
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
	fetch	next	from	@cSuit	into	@iParent,	@sServer,	@sServerQuoted,	@bIsAsyncronous
	if	@@fetch_status<>	0	break
----------
	select	@sPostixUnique=	replace ( replace ( replace ( replace ( convert ( varchar ( 24 ) , getdate() , 121 ) , '-' , '' ) , ' ' , '' ) , ':' , '' ) , '.' , '' )
		,@sProcName=	quotename ( '##'+	@sDBLogShippedSign+	'_PlanBR_'+	replace ( @@servername , '\' , '!' )+	@sPostixUnique )	-- ! ��������������� � ������ �����
----------
		set	@sScript=	'create	proc	'+	@sProcName+	'
	@sDBList	varchar ( 8000 )	output
as
set	@sDBList=	null'
----------
	set	@cBodySteps=	cursor	local	fast_forward	for
					select
						Action
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
						,Server
						,ServerQuoted
						,DB
						,DBQuoted
					from
						#Body
					where
						Parent=	@iParent
					order	by
						Id
----------
	open	@cBodySteps
----------
	while	1=	1
	begin
		fetch	next	from	@cBodySteps	into	@sAction,	@sFromServer,	@sFromDB,	@sFromFolder,	@sFromFile,	@sToServer,	@sToDB,	@sToFolder,	@sToFile,	@bIsCompressed,	@bIsCopyOnly,	@sServer,	@sServerQuoted,	@sDB,	@sDBQuoted
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
	set	@cServers=	cursor	local	fast_forward	for
		select
			ServerQuoted
		from
			#Body
		where
			Parent=		@iParent
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
			,b.Cutoff						-- ���������� ����������� �� ����� ����� �����, �.�. ��� ������ ���� �������� ���� � ����� ������
----------
	while	1=	1							-- ����, �.�. �������, ��� � @sBackupDir ����� ����� ��������� �������, ��� �������� ������������ restore
	begin
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
				set	@sMessage=	'������ ��������� �������� ������'
				goto	error
			end
----------
			update
				#BackupDir
			set
				ServerQuoted=	@sServerQuoted
			where
				ServerQuoted=	''			-- �������� null ��� ���������� �������
			if	@@error<>	0
			begin
				set	@sMessage=	'������ ��������� �������� ������'
				goto	error
			end
		end
----------
		close	@cServers
----------
		truncate	table	#BackupDirParsed
----------





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
			#BackupDirParsed	( ServerQuoted,	DB,	FileName,	Cutoff,	Extension,	IsRestoring )	-- ������ ���� ����� ���������� #BackupDir
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
			and	c2.Extension=		@sExtBak		-- ���� �� ���� ���� ����� ��� �����, �� .trn ��� �� �����, �.�. �� ��� ������ .bak
			inner	join	( select
						DB
						,DBQuoted
						,Server
						,ServerQuoted
					from
						#Body
					where
						Parent=		@iParent
					group	by
						DB
						,DBQuoted
						,Server
						,ServerQuoted )	d	on
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
			open	@cDatabases	-- ������ ���� ����� ���������� #BackupDirParsed
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
						if	@bQuorum=	0	set	@bStopWaiting=	1	else	set	@bQuorum=	0	-- ���� ������ ��� �� ������� ���-�� ������� ��� ����������� ��������������, �� ������� � ������������, ���� �� ������, �� ��������� ������
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
				truncate	table	#LabelOnly					-- ��-�� ������� continue � ������ ������� ����� ��
----------
				insert	#LabelOnly							-- ��� job� ������� ������ �� ���������� �����, �������, ��������, ��� ���������� �����������
				exec	( @sScriptTemp )						-- ������� �� restore ������ �������� � insert
				if	@@error<>	0	continue				-- ����� �� backup ����
----------
				set	@sScriptTemp=	'restore	headeronly	from	disk=	'+	@sBackupDirFileQuoted
----------
				if	@bDebug=	1
					print	( @sScriptTemp )
----------
				truncate	table	#BackupHeader
----------
				insert	#BackupHeader
				exec	( @sScriptTemp )						-- ������� �� restore ������ �������� � insert
				if	@@error<>	0	continue				-- ����� ���������?
----------
				select
					@iBackupType=		bh.BackupType
					,@sDBLive=		sd1.name
					--,@sDBStandby=		isnull ( sd2.name , sd1.name )
					,@dFirstLSN=		bh.FirstLSN
					,@databaseBackupLSN=	bh.databaseBackupLSN
				from
					#BackupHeader	bh
					inner	join	sysdatabases	sd1	on
						sd1.name=	bh.databaseName
					left	join	sysdatabases	sd2	on
						sd2.name=	@sDBLogShippedSign+	bh.databaseName
				where
						isnull ( sd2.name , sd1.name )=	@sDBStandby
				if	@@RowCount=	1						-- ��������� ���������� �������� ���� � ����� �����, ��������, 'DB' � 'DB1', ����� 1- ����� �������� ���� ��� ����������� � ����� �����?
				begin
					select	@sLockName=	upper ( @sDBStandby )+	@sProjectSign	-- ������������ � �������� ����, � ���� � �� �� ���� ����� �� ������ �������� ����� ��� � ������ ��������
----------
					select
						@sScript=	@sScript+	'
----------
exec	@iError=	sp_getapplock						-- ��������� ������, ���� ����� ��������� �������������� ������, �� ���������� ����� ��������� � @LockMode=Exclusive
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
														and	d2.Extension=	@sExtBak		-- ���� ���� ����������� .bak ����� ���� ����, �� ������������� .trn � .bak ��������������� �� �����
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
						if	@@error<>	0	continue	-- ����� ������ ������?
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
										end+	right ( PhysicalName , charindex ( '\' , reverse ( PhysicalName ) )-	1 )+	''''	-- �������� ��� ����� �� ������� ����
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
												and	db_name ( database_id )=@sDBStandby )	-- ��������� backup �� �������� ��� ���� �� ������� ��������������
								begin
									set	@sMessage=	'������� �������������� backup �������'
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
						if		@sDBStandby<>	@sDBLive							-- ����� �������� � ������� �����, ���� Standby � Live ���� ������
							and	not	exists	( select	1	from	#DBStandbyLive	where	DBLive=	@sDBLive )
							insert	#DBStandbyLive	( DBStandby,	DBLive )					-- ���������� ����������������� ����
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
						,StandbyFileName=	sbf.filename
						,LiveFileName=		lif.filename
						,TempFileName=		replace ( convert ( varchar ( 36 ) , newid() ) , '-' , '' )
					from
						#DBStandbyLive	d
						,sysdatabases	sb
						,sysaltfiles	sbf
						,sysdatabases	li
						,sysaltfiles	lif
					where
							sb.name=		d.DBStandby
						and	li.name=		d.DBLive
						and	sbf.database_id=	sb.dbid
						and	lif.database_id=	li.dbid
						and	sbf.fileid=		lif.fileid
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
		,'''+	isnull ( @sSQLDir , left ( StandbyFileName , len ( StandbyFileName )-	charindex ( '\' , reverse ( StandbyFileName ) ) ) )+	TempFileName+	''''	--�������� ��� ����� �� ������� ���� � �������� ����� ��� live ����, �� � ������ ������, ����� live ���� �� �������������
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
exec	sp_OAMethod	@iFSO,	''DeleteFile'',	'''+	@sSQLDir+	right ( LiveFileName , charindex ( '\' , reverse ( LiveFileName ) )-	1 )+	''''	--��������������� ������������� ��� ������ ������ ����� � ����� Live ����
					from
						#files
----------
					select
						@sScript=	@sScript+	'
----------
exec	sp_OAMethod	@iFSO,	''MoveFile'',	'''+	@sSQLDir+	TempFileName+	''',	'''+	@sSQLDir+	right ( LiveFileName , charindex ( '\' , reverse ( LiveFileName ) )-	1 )+	''''	--��������������� ������������� ��� ������ ������ ����� � ����� Live ����
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
					select						-- ���� � standby ������ ������� sp_changedbowner
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
exec	@iError=	sp_releaseapplock		-- ��������� ����������
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
			,@sBackupInfoStr=	replace ( @sBackupInfo , '''' , '''''' )	-- ����� ��� ���������� ������� ������ ����������
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
select							-- �������, ��� ������ ������������ �������� ��������� ���, ���� ������ � ��� ��������������� � ��� ���������� ������ � ������ ����������
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
/*
		exec	@sProcName
				@sDBList=	@sDBListOut2	output
*/
----------
		set	@sDBListOut=	@sDBListOut+	isnull ( @sDBListOut2+	@sDBDelimeter , '' )

	end
end
----------
deallocate	@cSuit
















----------
goto	done

error:

if	@sMessage	is	null	set	@sMessage=	'������ ������� log shipping'
raiserror ( @sMessage , 18 , 1 )
--EXEC	@iError=	sp_OAGetErrorInfo	null,	@source	OUT,	@desc	OUT
--SELECT	OLEObject=	CONVERT ( binary ( 4 ),	@iError ),	source=	@source,	description=	@desc

done:

set	@sDBList=	@sDBListOut

----------
--if	cursor_status ( 'variable' , '@cTemp' )<>	-2	deallocate	@cTemp

--drop	table
--	#BackupDir
