GO
RECONFIGURE
GO
exec	sp_configure 'show advanced options',		1
GO
exec	sp_configure 'Ole Automation Procedures',	1
GO
RECONFIGURE
GO
use	master
go
if	object_id ( 'dbo.BackupRestore' , 'p' )	is	null
	exec	( 'create	proc	dbo.BackupRestore	as	select	ObjectNotCreated=	1/0' )
go
/*
(c) 2017 TUnknown
License:
public domain as executing code, cc0 as citation
*/
alter	proc	dbo.BackupRestore	-- ����� � �������������� ��� ������
	@tPlan		text			-- maintenance ���� � XML
	,@tMessage	text=	null	output	-- ������ ��������� � XML, ��������, ������ � ��, ��� �� ���� � �����, �� ��� �� ������������
as
-- ��������� ��������� ��������� ������� � ������ ������
-- ������� ��� ��������� ��� ��������� �� linked ������� � ������ � �� xml ���������, ����������� ������ � ���� �������
set	nocount	on
declare	@iError			int
	,@iRowCount		int
	,@sMessage		varchar ( 256 )
	,@bDebug		bit	-- 1=�������� ���������� ���������

	,@iOLEConnection	int
	,@sMessage1		VARCHAR ( 4000 )
	,@sConnection		VARCHAR ( 4000 )
	,@sLogin		varchar ( 128 )
	,@sPwd			varchar ( 128 )
	,@iDumb			bigint

	,@sBackupFile		varchar ( 256 )
	,@sBackupDirFileQuoted	varchar ( 256 )

	,@iBackupType		int
	,@sDBStandby		varchar ( 256 )
	,@sDBLive		varchar ( 256 )
	,@sCutoff		varchar ( 256 )

	,@dFirstLSN		numeric ( 25 , 0 )
	,@databaseBackupLSN	numeric ( 25 , 0 )

	,@sScript		varchar ( max )			-- �� ���������� � sql 2000
	,@sScriptPart		nvarchar ( max )
	,@sScriptTemp		nvarchar ( max )

	,@cTemp			cursor
	,@cSuit			cursor
	,@cBodySteps		cursor

	,@bRestoring		bit
	,@bStopWaiting		bit
	,@bQuorum		bit
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
	,@sPostfixUnique	varchar ( 256 )
	,@sProcName		sysname

	,@iDBCount		int		-- ����� ��� � backup, ����������������� ���������
	,@dtMoment		datetime
	,@sDBDelimeter		varchar ( 2 )

	,@sFileNameBak		varchar ( 256 )

	,@iXML			integer
	,@iParent		smallint

	,@sSQLDir		nvarchar ( 260 )	-- (		restore	)������� � �������(mdf/ndf/ldf) ��� ������ MSSQL
	,@sDBList		varchar ( 8000 )
	,@bIsCoupled		bit		-- ->Cutoff( backup	restore	)backup=��������� ���� ��������� ���������;restore=���� ������������ ������, ���� ����� �� �������, �� �� �����������; �������� � ������ ������ ������ ������������
	,@sOwner		sysname		-- (		restore	)�������� ���� ������� ���� ���� ����������������� ��� logshipping ��� readonly

	,@bIsAsyncronous	bit

	,@sSuit			sysname

	,@iStep			smallint
	,@sAction		varchar ( 256 )	-- ������� BACKUP DATABASE/BACKUP LOG/restore
	,@sFromServer		sysname
	,@sFromDB		sysname
	,@sFromFolder		sysname
	,@sFromFile		sysname
	,@sToServer		sysname
	,@sToDB			sysname
	,@sToFolder		sysname
	,@sToFolderFinal	sysname
	,@sToFile		sysname
	,@bIsCompressed		bit		-- ( backup		)������������ ������ sqlserver, ���� �������, ��� ���������; ������� ������ ����� ��� �������� �� ��������� ����, ����� ������� �� ������ ��������� ���������� ��������
	,@bIsCopyOnly		bit		-- ( backup		)������ �����, �� ���������� ������� backup log
	,@sServer		sysname
	,@sServerQuoted		sysname
	,@sDB			sysname
	,@sDBQuoted		sysname
	,@sAccess		varchar ( 256 )

	,@iSequenceMax		smallint

	,@dLSNFirst		numeric ( 25,	0 )
	,@dLSNLast		numeric ( 25,	0 )

	,@x			xml
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
	,Suit		sysname		null		--backup/restore		-- ""=�������������� ����� �����������, ��� backup- ����+�����, ��� restore- ��������� �� FileName ��� ���� � ��������������� �� ��� ����� �� ����, � ��� ����� ������ suit; ������� ����� ��� ��������� ������, �������� ��� ������ ��� ����� Cutoff � ��������������� ��� ������ � ����� ����� Cutoff, ���� �� ������� �����, �� �� ��������������� �� ������; ���������� ������ asc
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
	,ToFolder	sysname		null		--backup
	,ToFolderFinal	sysname		null		--backup/restore
	,ToFile		sysname		null		--backup
	,IsCompressed	bit		null		--backup
	,IsCopyOnly	bit		null		--backup
	,ProcName	sysname		null		--backup/restore
,check	( Action	in	( 'backup database' , 'backup log' , 'restore' ) )	)
----------
create	table	#Body	-- ���������� ������
(	Id		smallint	not null	unique	clustered	--backup/restore
	,Parent		smallint	not null	foreign	key	references	#Suit ( Id ) --backup/restore, ��� �����������, �� �����- FK �� ��������

	,Action		varchar ( 256 )	null		-- backup database/backup log/restore; ��� backup ��������� �������� �� FromServer, ��� restore ��������� �������� �� ToServer, � �������� ������ Suit ��� ���� ������ ���� ��������� ���������
	,FromServer	sysname		null		--backup/restore		��� ������� �� ������ ������, ��������, ���� ���� � ����� ������ ���������� �� 2� ��������
	,FromDB		sysname		null		--backup/?restore
	,FromFolder	sysname		null		--restore
	,FromFile	sysname		null		--restore		-- �������� ��� ���� � ������� � ����� ����� �� ������ ������ � ���� ������� ������������ ��� CutOff
	,ToServer	sysname		null		--restore
	,ToDB		sysname		null		--restore
	,ToFolder	sysname		null		--backup
	,ToFolderFinal	sysname		null		--backup/restore	-- ������� � ���� ������ ����� backup/restore, ��� ����, ����� ��� ����� ���� ��������������� ���������� ���������
	,ToFile		sysname		null		--backup
	,IsCompressed	bit		null		--backup
	,IsCopyOnly	bit		null		--backup
	,LSNFirst	numeric ( 25,	0 )	null	--restore
	,LSNLast	numeric ( 25,	0 )	null	--restore
	,Access		varchar ( 256 )	null		--restore		null= �� ��������� ����� ����, read=��������� �����, ���� ��������� � readonly, write=��������� �����, ��� ���� ��������� ������
	,Message	varchar ( max )	null		-- ����� ������ ��� ������� ��������� ���� ������
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
	#Suit	( Id,	Suit,	IsAsyncronous,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFolderFinal,	ToFile,	IsCompressed,	IsCopyOnly )
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
FROM
	OPENXML	( @iXML,	'/PlanBR/s',	1 )	-- �������� case sensitive
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
	,IsCopyOnly	bit )
----------
-- ����� �������� ������ ���� check � unique?
insert
	#Body	( Id,	Parent,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFolderFinal,	ToFile,	IsCompressed,	IsCopyOnly,	Access )
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
	,Access
from
	OPENXML	( @iXML,	'/PlanBR/s/b',	1 )	-- �������� case sensitive
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
	,Access		varchar ( 256 ) )
----------
EXEC	@iError=	sp_xml_removedocument	@iXML	-- sql2000+
if	@@Error<>	0	or	@iError<>	0
begin
	select	@sMessage=	'������ XML 2',
		@iError=	-3
	goto	error
end
----------
update
	b
set
	@sMessage=	isnull ( @sMessage , '' )
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
create	table	#File_BackupHeader	-- ����� �� ��������� master.dbo.sp_can_tlog_be_applied
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
if		convert ( varchar ( 10 ) , serverproperty ( 'ProductVersion' ) )	like	'%.%'	-- ��������� ������ �������
	and	10<	left ( convert ( varchar ( 10 ) , serverproperty ( 'ProductVersion' ) ) , charindex ( '.' , convert ( varchar ( 128 ) , serverproperty ( 'ProductVersion' ) ) ) - 1 )
begin
	alter	table	#File_LabelOnly		add
		MirrorCount		int	-- � ������������ ���� ���������� Mirror_Count
		,IsCompressed		bit
----------
	alter	table	#File_BackupHeader	add
		CompressedBackupSize	bigint
		,containment		tinyint
----------
	alter	table	#File_FileListOnly	add
		TDEThumbprint		varbinary ( 32 )
----------
	set	@sPattern=		'%2[0-9][0-9][0-9][_][0-1][0-9][_][0-3][0-9][_][0-2][0-9][0-5][0-9][0-5][0-9][_][0-9][0-9][0-9][0-9][0-9][0-9][0-9].%'	-- ����� ����� backup ����� �� MaintenancePlan sql 2012
end
else
begin
	alter	table	#File_LabelOnly		add	-- ���, ���� linked ������ ������, ������������� � ��������?
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



/*
		select
			@sDBListOut=	isnull ( @sDBListOut , '' )/*+	t.Server*/+	t.DB+	@sDBDelimeter
		from
			#Body	t
			left	join	sysservers	ss	on
				ss.srvname=	t.Server
		where
				t.Parent=	@iParent
			and	(	ss.isremote=	1			-- remote ������� �� ��������������, ������ linked
				or	(	t.Server	is	not	null
					and	ss.srvname	is		null ) )
		group	by
			--t.Server					-- ���������� �� �������� ������ ����
			t.DB
		order	by
			min ( Id )					-- ���������� ����� �� ���������
*/



----------
update
	b
set
	b.FromDB=	case
				when	b.Action	like	'backup%'	then	isnull ( d.name/*����� ��� �����?*/ , b.FromDB )
				else							b.FromDB
			end
	,b.ToDB=	case
				when	b.Action	like	'backup%'	then	b.ToDB
				else							isnull ( d.name , b.ToDB )
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
				else							b.ToFile
			end
from
	#Body	b
	inner	join	sysdatabases	d	on		-- ���������� ��� �������������� ����, ����� �� ������������� �� �� ������?
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
	exec	xp_dirtree		-- xp_cmdshell �� ��� �����������, �.�. wildcard � dir �������� �� ������� � �������� ����� ������, ��� ����� ���������� ����������, ��������, ��� ����� ��������������� �� ~1 � 1
			@sFromFolder
			,0
			,1
	if	@@error<>	0
	begin
		set	@sMessage=	'������ ��������� �������� ������'
		goto	error
	end
----------
	delete				-- ��������� ������ �����, ����������� � backup �������� ���� ������ �� ���������� ���������� ����� �����, �������� �����, ���� ���� ��� ��������� �������� ������, ��������, DB � DB1
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
			ServerQuoted=	''			-- �������� null ��� ���������� �������
		and	Folder=		''
	if	@@error<>	0
	begin
		set	@sMessage=	'������ ��������� �������� ������'
		goto	error
	end
end
----------
if	0<	@@CURSOR_ROWS
begin
	select	@iSequenceMax=	max ( Id )	from	#Body
----------
	insert
		#Body	( Id,	Parent,	Action,	FromServer,	FromDB,	FromFolder,	FromFile,	ToServer,	ToDB,	ToFolder,	ToFolderFinal,	ToFile,	IsCompressed,	IsCopyOnly,	Access )
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
		and	d.FileName	like	'%'+	b.FromDB+	'%'	-- �����, ����������� � backup ����(ToDB ����� ����� ������ ���) ������ �� ���������� ���������� ����� �����, �������� �����, ���� ���� ��� ��������� �������� ������, ��������, DB � DB1
		and	b.FileName	is		null
		and	1<		d.Sequence			-- ��������� ������ �������� ��� ����, ����� FileName
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
		and	d.FileName	like	'%'+	b.FromDB+	'%'	-- �����, ����������� � backup ����(ToDB ����� ����� ������ ���) ������ �� ���������� ���������� ����� �����, �������� �����, ���� ���� ��� ��������� �������� ������, ��������, DB � DB1
		and	b.FileName	is	null
		and	d.Sequence=	1				-- ������������ ���� ������ ���������
end
----------
deallocate	@cTemp	-- ��������� ����� ���������� ��������� � @@CURSOR_ROWS
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
	truncate	table	#File_LabelOnly					-- ��-�� ������� continue � ������ ������� ����� ��
----------
	insert	#File_LabelOnly							-- ��� job� ������� ������ �� ���������� �����, �������, ��������, ��� ���������� �����������
	exec	( @sScriptTemp )						-- ������� �� restore ������ �������� � insert
	select	@iError=	@@Error
		,@iRowCount=	@@RowCount
	if	@iError<>	0	or	@iRowCount<	1
	begin
		update
			#Body
		set
			Message=	isnull ( Message+	'; ',	'' )+	'������ restore labelonly'
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
	exec	( @sScriptTemp )						-- ������� �� restore ������ �������� � insert
	select	@iError=	@@Error
		,@iRowCount=	@@RowCount
	if	@iError<>	0	or	@iRowCount<	1
	begin
		update
			#Body
		set
			Message=	isnull ( Message+	'; ',	'' )+	'������ restore headeronly'
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
	if	@@RowCount<>	1
		update
			#Body
		set
			Message=	isnull ( Message+	'; ',	'' )+	'������ LSN headeronly'
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
		@dLSNFirst<=	min ( redo_start_lsn )	and	min ( redo_start_lsn )<	@dLSNLast	-- ��������, ������ ����� �� ������� ��������
	if	@@Error<>	0	or	@iRowCount<>	1
	begin
		update
			#Body
		set
			Message=	isnull ( Message+	'; ',	'' )+	'������ LSN headeronly'
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

--***��������� ���������� @iBackupType � ���������� �����












end
----------
deallocate	@cTemp
----------
update
	bN
set
	bN.Message=	isnull ( bN.Message+	'; ',	'' )+	'��� �����������'	-- ��������� ������������� ���� �� ���������
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
	and	( bN.ToFolderFinal=	b.ToFolderFinal	or	isnull ( bN.ToFolderFinal,	b.ToFolderFinal )	is	null )
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
		or	( bP.ToFolderFinal<>	b.ToFolderFinal	and	isnull ( bP.ToFolderFinal,	b.ToFolderFinal )	is	not	null )
		or	( bP.ToFile<>	b.ToFile	and	isnull ( bP.ToFile,	b.ToFile )	is	not	null )
		or	( bP.IsCopyOnly<>b.IsCopyOnly	and	isnull ( bP.IsCopyOnly,	b.IsCopyOnly )	is	not	null ) )
where
		bP.Id	is	null
----------









--***�������� ������ ������ Suit, ��������, ��� ��� dba+dbb+dbc ����� dba1+dbb1+dbc1 �������� ������ suit, � dba2+dbb2 ��������, ������� ������ ����� ��������� dbc2 ��� ����� �������. �������� ������ ��������� � ������ warning



--***������ ������� Suit ���� ������������� ������ ��� ����������� �������� ��������















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
		select	@sScriptTemp=	( select
						[data()]=	text								-- �������� ������� ������ ������������� ��� ������ ������� text()
					from
						syscomments
					where
							id=		@@procid
						and	number<>	0
						and	encrypted=	0
					for
						xml	path ( '' )
						,TYPE ).value ( '(./text())[1]',	'nvarchar(max)' )			-- ����� �� �������� xml �����������
			,@sProcName=	quotename ( isnull ( object_name ( @@procid ),	( select
												name
											from
												sysobjects			-- ������, ��� object_name ���� ��������� ��������� � ������� ����, � �� tempdb, �.�. �� ��������
											where
													id=	@@procid
												and	xtype=	'p' ) ) )
			,@sScript=	'create	proc	'+	/*schema_name ( objectproperty ( @@procid,	'OwnerId' ) )*/+	@sProcName+	'
	'+	right ( @sScriptTemp,	len ( @sScriptTemp )-	patindex ( '%[^a-z0-9]@[a-z0-9]%as%',	@sScriptTemp ) )	-- �������, ��� �������� � ��������� ����; �������� ��� ���������, �� ����� �� ���������, ����� ��������� ����� ��� ������������� linked �������
----------
		set	@x=	( select
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
				from
					#suit	s
					,#body	b
				where
						s.Id=		@iParent
					and	b.Parent=	s.Id
					and	b.Message	is	null	-- ���� ��� ������ ������, � �� ��������������
				order	by
					b.Id
				for
					xml	auto
					,root ( 'PlanBR' ) )
	end
	else
	begin
		select	@sPostfixUnique=	replace ( replace ( replace ( replace ( convert ( varchar ( 24 ) , getdate() , 121 ) , '-' , '' ) , ' ' , '' ) , ':' , '' ) , '.' , '' )
			,@sProcName=		quotename (	case
									when	@sServer	is	null	then	'##'	-- ������ �� ��������� ������� ��������� ��������� ������� �� �������� �� ����������
									else						''
								end
							+	@sPostfixUnique
							+	'_PlanBR_'
							+	replace ( @@servername , '\' , '!' )
							+	@sDBLogShippedSign )	-- ! ��� ������������ � ������ �����
			,@sScript=		'
create	proc	'+	@sProcName+	'
	@sDBList	varchar ( 8000 )	output
as
set	nocount	on
----------
drop	proc	'+	@sProcName+	'	-- ����� ��� ���������� ��������, ����� ������� � �����, ����� ����� ��������� ����� ���� �������� �� � ����� ����������, �� ����� ���� ���� ���������� �� ������ � �� ����� �� �����
----------
declare	@iFSO		int
	,@iError	int
----------
set	@sDBList=	null'
----------

--***���������� � ��������� BodyId





----------
		set	@cBodySteps=	cursor	local	forward_only	for			-- ������ �������������� ������ ��������, �.�. �� ���������� ���������� @iParent
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
												or	bP.Server<>	b.Server
												or	bP.DB<>		b.DB
												or	bP.Id	is	null	then	1
											else						0
										end
							,BeforeDBChange=	case
											when		bN.Action<>	b.Action
												or	bN.Server<>	b.Server
												or	bN.DB<>		b.DB
												or	bN.Id	is	null	then	1
											else						0
										end
							,AfterServerChange=	case
											when		bP.Action<>	b.Action
												or	bP.Server<>	b.Server
												or	bP.Id	is	null	then	1
											else						0
										end
							,BeforeServerChange=	case
											when		bN.Action<>	b.Action
												or	bN.Server<>	b.Server
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
							and	b.Message	is	null	-- ���� ��� ������ ������, � �� ��������������
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
			if		@sAction=	'restore'
				and	exists	( select	-- ���� backup �� �������� ��� ���� �� ������� ��������������, �� ���������� ���- ������� �� � ������ ������ ����������
							1
						from
							sys.master_files	-- sysaltfiles
						where
								@dLSNFirst<=	isnull ( redo_start_lsn,	differential_base_lsn )	and	isnull ( redo_start_lsn,	differential_base_lsn )<	@dLSNLast
							and	db_name ( database_id )=	@sDBStandby )
			begin
				select
					@iDBCount=	count (	distinct	DB )
				from
					#Body
				where
					Parent=		@iParent
----------
				set	@sScriptPart=	''
----------
				select
					@sScriptPart=	@sScriptPart+	'
----------
EXEC	sp_OACreate	''Scripting.FileSystemObject'',	@iFSO	OUT'
				where
					@bAfterServerChange=	1
----------

/*				if	@bIsCoupled=	1
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
				end*/
----------

-- ������� ����, ���� ��� �� ���������� � ��������� � � message?



--@sDBStandby	�����������������(��������� �������� ���), �������� � @sDBLive	� ������� ������
--@sDBLive	��������� ��� ��������������, ������, �����			� �������� ������




--				if	@@RowCount=	1						-- ��������� ���������� �������� ���� � ����� �����, ��������, 'DB' � 'DB1', ����� 1- ����� �������� ���� ��� ����������� � ����� �����?
				begin
					select	@sLockName=	upper ( @sDBLive )+	@sProjectSign	-- ������������ � �������� ����, � ���� � �� �� ���� ����� �� ������ �������� ����� ��� � ������ ��������
----------
					select
						@sScriptPart=	@sScriptPart+	'
----------
exec	@iError=	master..sp_getapplock					-- ��������� ������; ���� ����� ��������� ����������� � �������������� ����, �� ���������� ����� ��������� � @LockMode=Exclusive, ��������, ��� ���������� ����������� ������
				@Resource=	'''+	@sLockName+	'''	-- ������ ��� ���������� ������������ master ������� ������, �.�. ���������� ����������� ������������ ������� ����, ������� � ��������� �� ����� ������������ �������� ����, ���� ����� ������ � ����� execute
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

***����� ��������, ��� ���� ���� ����� ������� .bak, �� ���������� .trn �� ���������������, �������� ��, ��� ��������� ����� ���������� � suit

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
						if	@@error<>	0	continue	-- ����� ������ ������? ����� �� ������������ ������
----------
						select
							@sSQLDir=	left ( filename,	len ( filename )-	charindex ( '\',	reverse ( filename ) ) )+	'\'
						from
							sysdatabases
						where
							name=	@sDB
----------
						set	@sScriptTemp=	'
select
	sdb.dbid
	,saf.fileid
	,sdb.name
	,LogicalName=	saf.name
	,saf.filename
	,TempName=	replace ( convert ( varchar ( 36 ) , newid() ) , ''-'' , '''' )	-- �������� ���� �� ��� �������
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
						exec	( @sScriptTemp )
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
						if	@sExtension=	@sExtBak
						begin
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


if	@bDebug=	1
begin
	select * from #Generation_Files
	select * from #File_FileListOnly
	select * from #Server_DBFiles
end



							select
								@sScriptPart=	@sScriptPart+	'
	,move	'''+	flo.LogicalName+	'''	to	'''+	isnull ( left ( saf.filename , len ( saf.filename )-	charindex ( '\' , reverse ( saf.filename ) )+	1 ) , @sSQLDir )	-- ���� ����� ����������������� ��� ������ ���������
									+	case	@sDBLive
											when	@sDBStandby	then	''
											else				@sDBLogShippedSign
										end
									+	right ( flo.PhysicalName , charindex ( '\' , reverse ( flo.PhysicalName ) )-	1 )+	''''	-- �������� ��� ����� �� ������� ����
							from
								#File_FileListOnly	flo
								left	join	#Server_DBFiles	saf	on
									saf.LogicalName=	flo.LogicalName
								and	saf.DBName=		@sDB
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
'+	case	@bRestoring		-- ���� ������������������� ������ ����� �������, �.�. ��� ������� � ���� �� ����������� ����� ����� ������ ��������������; � ����������� ������ ��� ������ ��������� ������� �������, ��������, ����� ��������
		when	1	then	'if	@@Error=	0
	'
		else			''
	end+	'exec	sp_OAMethod	@iFSO,	''DeleteFile'',	null,	'+	@sBackupDirFileQuoted+	',	1'	-- ***�� �������, ���� �� ����� ����� ����� ��������������� ��� ���� � ������ ������ �� ���� �� �������
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
		,'''+	isnull ( @sSQLDir,	StandByPath )+	TempFileName+	''''	--�������� ��� ����� �� ������� ���� � �������� ����� ��� live ����, �� � ������ ������, ����� live ���� �� �������������
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
exec	sp_OAMethod	@iFSO,	''DeleteFile'',	'''+	@sSQLDir+	LiveFile+	''''	--��������������� ������������� ��� ������ ������ ����� � ����� Live ����
					from
						#Generation_Files
					where
						@sAccess	is	not	null
----------
					select
						@sScriptPart=	@sScriptPart+	'
----------
exec	sp_OAMethod	@iFSO,	''MoveFile'',	'''+	@sSQLDir+	TempFileName+	''',	'''+	@sSQLDir+	LiveFile+	''''	--��������������� ������������� ��� ������ ������ ����� � ����� Live ����
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
exec	'+	@sDBLive+	'..sp_changedbowner	'''+	'dbo'/*@sOwner*/+	''''	-- ���� � standby ������ ������� sp_changedbowner
					where
						@sAccess	is	not	null
				end
----------
				select
					@sScriptPart=	@sScriptPart+	'

skip_'+	upper ( @sDBLive )+	':

exec	@iError=	master..sp_releaseapplock		-- ��������� ����������
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
----------------------------------------------------------------------------------------------------
			else
				if		@sAction	like	'backup database'
					or	@sAction	like	'backup log'
				begin
					select	@dtMoment=		getdate()
						,@sCutoff=		str ( year ( @dtMoment ) , 4 )
								+	'_'
								+	replace ( str ( month ( @dtMoment ) , 2 ) , ' ' , '0' )
								+	'_'
								+	replace ( str ( day ( @dtMoment ) , 2 ) , ' ' , '0' )
								+	'_'
								+	replace ( replace ( right ( convert ( varchar ( 23 ) , @dtMoment , 121 ) , 12 ) , ':' , '' ) , '.' , '_' )
								--+	'0000'
						,@sBackupInfo=		@sCutoff
								+	'|'
								+	convert ( varchar ( 128 ) , convert ( varchar ( 16 ) , SERVERPROPERTY ( 'ProductVersion' ) )
								+	'|'
								+	convert ( varchar ( 8 ) , SERVERPROPERTY ( 'EngineEdition' ) )
								+	'|'
								+	@@servername+	isnull ( '.'+	db_name()+	'.'+	schema_name ( OBJECTPROPERTY ( @@procid,'OwnerId' ) )+	'.'+	object_name ( @@procid ) , '' )
								+	'|'
								+	convert ( varchar ( 2 ) , @@NESTLEVEL )
								+	isnull ( '|'+	app_name() , '' ) )
						,@sBackupInfoStr=	replace ( @sBackupInfo , '''' , '''''' )	-- ����� ��� ���������� ������� ������ ����������
----------
					set	@sScriptPart=		'
----------
select							-- �������, ��� ������ ������������ �������� ��������� ���, ���� ������ � ��� ��������������� � ��� ���������� ������ � ������ ����������
	@sDBList=	isnull ( @sDBList , '''' )+	DB_NAME ( database_id )+	'''+	@sDBDelimeter+	'''
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
										else			''					-- �� ��������� �������� �� ���������
									end
								+	case	@bIsCopyOnly
										when	1	then	'
		,COPY_ONLY'
										else			''
									end
							+	'
	if	@@Error<>	0
		set	@sDBList=	isnull ( @sDBList , '''' )+	'''+	@sDB+	@sDBDelimeter+	'''
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
		print	substring(@sScript,1,8000)
		print	substring(@sScript,8001,16000)
		print	substring(@sScript,16001,24000)
		print	substring(@sScript,24001,32000)
		print	substring(@sScript,32001,40000)
		print	substring(@sScript,40001,48000)
		print	substring(@sScript,48001,56000)
		print	substring(@sScript,56001,64000)
		print	substring(@sScript,64001,72000)
		print	substring(@sScript,72001,80000)
	end
----------
	if	@sServer<>	@@ServerName
	begin
		select	@sScript=	'
exec ( '+	replace ( @sScript,	'''',	'''''' )+	')
exec	'+	@sProcName+	'
		@tPlan=		@x
		,@tMessage=	@s	output'
			,@sExecAtServer=	@sServerQuoted+	'.tempdb..sp_executesql'	-- ����� ��������� ��� ## �� ������ ������� ����������� � tempdb, � �� � ���� �� ��������� ��� ������ ������ �������
----------
		exec	@sExecAtServer
				@statement=	@sScript
				,@params=	N'@x	xml,	@s	varchar ( 8000 )	out'
				,@x=		@x
				,@s=		@sDBList	out
	end
	else
		exec	( @sScript )				-- ������� ������� ��� ���������, ����� ��������� ��, �������� ����������
----------
	update
		#Suit
	set
		ProcName=	@sProcName
	where
		Id=		@iParent
----------
	set	@sDBListOut=	@sDBListOut+	isnull ( @sDBListOut2+	@sDBDelimeter , '' )
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


-- ***����� ��������- �� ��������� ������������ �������, ���� ��� �������� � �� ����������� �� ����� ���������� ���� � affinity

		select	@sLogin=	null
			,@sPwd=		null
			,@sConnection=	'Provider=SQLOLEDB;'	-- ���� ���� ��������� �����������?
				+	'Server='+		isnull ( @sServer,	@@servername )+	';'
				+	'Database=tempdb;'
				+	'Trusted_Connection='+	case
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
						,@iDumb		output	-- ��� ����� ���� ������ ������� ������ ������������ ���� 0x80020005, ������� ����� ��������������� ��-�� ����, ��� �������� ��� Recordset ��� �����������
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