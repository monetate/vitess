/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlerror

import (
	"strconv"
	"strings"
)

type ErrorCode uint16

func (e ErrorCode) ToString() string {
	return strconv.FormatUint(uint64(e), 10)
}

// Error codes for server-side errors.
// Originally found in include/mysql/mysqld_error.h and
// https://dev.mysql.com/doc/mysql-errors/en/server-error-reference.html
// The below are in sorted order by value, grouped by vterror code they should be bucketed into.
// See above reference for more information on each code.
const (
	// Vitess specific errors, (100-999)
	ERNotReplica       = ErrorCode(100)
	ERNonAtomicCommit  = ErrorCode(301)
	ERInAtomicRecovery = ErrorCode(302)

	// unknown
	ERUnknownError = ErrorCode(1105)

	// internal
	ERInternalError = ErrorCode(1815)

	// unimplemented
	ERNotSupportedYet = ErrorCode(1235)
	ERUnsupportedPS   = ErrorCode(1295)

	// resource exhausted
	ERDiskFull               = ErrorCode(1021)
	EROutOfMemory            = ErrorCode(1037)
	EROutOfSortMemory        = ErrorCode(1038)
	ERConCount               = ErrorCode(1040)
	EROutOfResources         = ErrorCode(1041)
	ERRecordFileFull         = ErrorCode(1114)
	ERHostIsBlocked          = ErrorCode(1129)
	ERCantCreateThread       = ErrorCode(1135)
	ERTooManyDelayedThreads  = ErrorCode(1151)
	ERNetPacketTooLarge      = ErrorCode(1153)
	ERTooManyUserConnections = ErrorCode(1203)
	ERLockTableFull          = ErrorCode(1206)
	ERUserLimitReached       = ErrorCode(1226)

	// deadline exceeded
	ERLockWaitTimeout = ErrorCode(1205)

	// unavailable
	ERServerShutdown = ErrorCode(1053)

	// not found
	ERDbDropExists          = ErrorCode(1008)
	ERCantFindFile          = ErrorCode(1017)
	ERFormNotFound          = ErrorCode(1029)
	ERKeyNotFound           = ErrorCode(1032)
	ERBadFieldError         = ErrorCode(1054)
	ERNoSuchThread          = ErrorCode(1094)
	ERUnknownTable          = ErrorCode(1109)
	ERCantFindUDF           = ErrorCode(1122)
	ERNonExistingGrant      = ErrorCode(1141)
	ERNoSuchTable           = ErrorCode(1146)
	ERNonExistingTableGrant = ErrorCode(1147)
	ERKeyDoesNotExist       = ErrorCode(1176)

	// permissions
	ERDBAccessDenied            = ErrorCode(1044)
	ERAccessDeniedError         = ErrorCode(1045)
	ERKillDenied                = ErrorCode(1095)
	ERNoPermissionToCreateUsers = ErrorCode(1211)
	ERSpecifiedAccessDenied     = ErrorCode(1227)

	// failed precondition
	ERNoDb                          = ErrorCode(1046)
	ERNoSuchIndex                   = ErrorCode(1082)
	ERCantDropFieldOrKey            = ErrorCode(1091)
	ERTableNotLockedForWrite        = ErrorCode(1099)
	ERTableNotLocked                = ErrorCode(1100)
	ERTooBigSelect                  = ErrorCode(1104)
	ERNotAllowedCommand             = ErrorCode(1148)
	ERTooLongString                 = ErrorCode(1162)
	ERDelayedInsertTableLocked      = ErrorCode(1165)
	ERDupUnique                     = ErrorCode(1169)
	ERRequiresPrimaryKey            = ErrorCode(1173)
	ERCantDoThisDuringAnTransaction = ErrorCode(1179)
	ERReadOnlyTransaction           = ErrorCode(1207)
	ERCannotAddForeign              = ErrorCode(1215)
	ERNoReferencedRow               = ErrorCode(1216)
	ERRowIsReferenced               = ErrorCode(1217)
	ERCantUpdateWithReadLock        = ErrorCode(1223)
	ERNoDefault                     = ErrorCode(1230)
	ERMasterFatalReadingBinlog      = ErrorCode(1236)
	EROperandColumns                = ErrorCode(1241)
	ERSubqueryNo1Row                = ErrorCode(1242)
	ERUnknownStmtHandler            = ErrorCode(1243)
	ERWarnDataOutOfRange            = ErrorCode(1264)
	ERNonUpdateableTable            = ErrorCode(1288)
	ERFeatureDisabled               = ErrorCode(1289)
	EROptionPreventsStatement       = ErrorCode(1290)
	ERDuplicatedValueInType         = ErrorCode(1291)
	ERSPDoesNotExist                = ErrorCode(1305)
	ERNoDefaultForField             = ErrorCode(1364)
	ErSPNotVarArg                   = ErrorCode(1414)
	ERRowIsReferenced2              = ErrorCode(1451)
	ErNoReferencedRow2              = ErrorCode(1452)
	ERInnodbIndexCorrupt            = ErrorCode(1817)
	ERDupIndex                      = ErrorCode(1831)
	ERInnodbReadOnly                = ErrorCode(1874)

	ERVectorConversion = ErrorCode(6138)

	// already exists
	ERDbCreateExists = ErrorCode(1007)
	ERTableExists    = ErrorCode(1050)
	ERDupEntry       = ErrorCode(1062)
	ERFileExists     = ErrorCode(1086)
	ERUDFExists      = ErrorCode(1125)

	// aborted
	ERGotSignal          = ErrorCode(1078)
	ERForcingClose       = ErrorCode(1080)
	ERAbortingConnection = ErrorCode(1152)
	ERLockDeadlock       = ErrorCode(1213)

	// invalid arg
	ERUnknownComError              = ErrorCode(1047)
	ERBadNullError                 = ErrorCode(1048)
	ERBadDb                        = ErrorCode(1049)
	ERBadTable                     = ErrorCode(1051)
	ERNonUniq                      = ErrorCode(1052)
	ERWrongFieldWithGroup          = ErrorCode(1055)
	ERWrongGroupField              = ErrorCode(1056)
	ERWrongSumSelect               = ErrorCode(1057)
	ERWrongValueCount              = ErrorCode(1058)
	ERTooLongIdent                 = ErrorCode(1059)
	ERDupFieldName                 = ErrorCode(1060)
	ERDupKeyName                   = ErrorCode(1061)
	ERWrongFieldSpec               = ErrorCode(1063)
	ERParseError                   = ErrorCode(1064)
	EREmptyQuery                   = ErrorCode(1065)
	ERNonUniqTable                 = ErrorCode(1066)
	ERInvalidDefault               = ErrorCode(1067)
	ERMultiplePriKey               = ErrorCode(1068)
	ERTooManyKeys                  = ErrorCode(1069)
	ERTooManyKeyParts              = ErrorCode(1070)
	ERTooLongKey                   = ErrorCode(1071)
	ERKeyColumnDoesNotExist        = ErrorCode(1072)
	ERBlobUsedAsKey                = ErrorCode(1073)
	ERTooBigFieldLength            = ErrorCode(1074)
	ERWrongAutoKey                 = ErrorCode(1075)
	ERWrongFieldTerminators        = ErrorCode(1083)
	ERBlobsAndNoTerminated         = ErrorCode(1084)
	ERTextFileNotReadable          = ErrorCode(1085)
	ERWrongSubKey                  = ErrorCode(1089)
	ERCantRemoveAllFields          = ErrorCode(1090)
	ERUpdateTableUsed              = ErrorCode(1093)
	ERNoTablesUsed                 = ErrorCode(1096)
	ERTooBigSet                    = ErrorCode(1097)
	ERBlobCantHaveDefault          = ErrorCode(1101)
	ERWrongDbName                  = ErrorCode(1102)
	ERWrongTableName               = ErrorCode(1103)
	ERUnknownProcedure             = ErrorCode(1106)
	ERWrongParamCountToProcedure   = ErrorCode(1107)
	ERWrongParametersToProcedure   = ErrorCode(1108)
	ERFieldSpecifiedTwice          = ErrorCode(1110)
	ERInvalidGroupFuncUse          = ErrorCode(1111)
	ERTableMustHaveColumns         = ErrorCode(1113)
	ERUnknownCharacterSet          = ErrorCode(1115)
	ERTooManyTables                = ErrorCode(1116)
	ERTooManyFields                = ErrorCode(1117)
	ERTooBigRowSize                = ErrorCode(1118)
	ERWrongOuterJoin               = ErrorCode(1120)
	ERNullColumnInIndex            = ErrorCode(1121)
	ERFunctionNotDefined           = ErrorCode(1128)
	ERWrongValueCountOnRow         = ErrorCode(1136)
	ERInvalidUseOfNull             = ErrorCode(1138)
	ERRegexpError                  = ErrorCode(1139)
	ERMixOfGroupFuncAndFields      = ErrorCode(1140)
	ERIllegalGrantForTable         = ErrorCode(1144)
	ERSyntaxError                  = ErrorCode(1149)
	ERWrongColumnName              = ErrorCode(1166)
	ERWrongKeyColumn               = ErrorCode(1167)
	ERBlobKeyWithoutLength         = ErrorCode(1170)
	ERPrimaryCantHaveNull          = ErrorCode(1171)
	ERTooManyRows                  = ErrorCode(1172)
	ERErrorDuringCommit            = ErrorCode(1180)
	ERLockOrActiveTransaction      = ErrorCode(1192)
	ERUnknownSystemVariable        = ErrorCode(1193)
	ERSetConstantsOnly             = ErrorCode(1204)
	ERWrongArguments               = ErrorCode(1210)
	ERWrongUsage                   = ErrorCode(1221)
	ERWrongNumberOfColumnsInSelect = ErrorCode(1222)
	ERDupArgument                  = ErrorCode(1225)
	ERLocalVariable                = ErrorCode(1228)
	ERGlobalVariable               = ErrorCode(1229)
	ERWrongValueForVar             = ErrorCode(1231)
	ERWrongTypeForVar              = ErrorCode(1232)
	ERVarCantBeRead                = ErrorCode(1233)
	ERCantUseOptionHere            = ErrorCode(1234)
	ERIncorrectGlobalLocalVar      = ErrorCode(1238)
	ERWrongFKDef                   = ErrorCode(1239)
	ERKeyRefDoNotMatchTableRef     = ErrorCode(1240)
	ERCyclicReference              = ErrorCode(1245)
	ERIllegalReference             = ErrorCode(1247)
	ERDerivedMustHaveAlias         = ErrorCode(1248)
	ERTableNameNotAllowedHere      = ErrorCode(1250)
	ERCollationCharsetMismatch     = ErrorCode(1253)
	ERWarnDataTruncated            = ErrorCode(1265)
	ERCantAggregate2Collations     = ErrorCode(1267)
	ERCantAggregate3Collations     = ErrorCode(1270)
	ERCantAggregateNCollations     = ErrorCode(1271)
	ERVariableIsNotStruct          = ErrorCode(1272)
	ERUnknownCollation             = ErrorCode(1273)
	ERWrongNameForIndex            = ErrorCode(1280)
	ERWrongNameForCatalog          = ErrorCode(1281)
	ERBadFTColumn                  = ErrorCode(1283)
	ERTruncatedWrongValue          = ErrorCode(1292)
	ERTooMuchAutoTimestampCols     = ErrorCode(1293)
	ERInvalidOnUpdate              = ErrorCode(1294)
	ERUnknownTimeZone              = ErrorCode(1298)
	ERInvalidCharacterString       = ErrorCode(1300)
	ERQueryInterrupted             = ErrorCode(1317)
	ERViewWrongList                = ErrorCode(1353)
	ERTruncatedWrongValueForField  = ErrorCode(1366)
	ERIllegalValueForType          = ErrorCode(1367)
	ERDataTooLong                  = ErrorCode(1406)
	ErrWrongValueForType           = ErrorCode(1411)
	ERNoSuchUser                   = ErrorCode(1449)
	ERForbidSchemaChange           = ErrorCode(1450)
	ERWrongValue                   = ErrorCode(1525)
	ERWrongParamcountToNativeFct   = ErrorCode(1582)
	ERDataOutOfRange               = ErrorCode(1690)
	ERInvalidJSONText              = ErrorCode(3140)
	ERInvalidJSONTextInParams      = ErrorCode(3141)
	ERInvalidJSONBinaryData        = ErrorCode(3142)
	ERInvalidJSONCharset           = ErrorCode(3144)
	ERInvalidCastToJSON            = ErrorCode(3147)
	ERJSONValueTooBig              = ErrorCode(3150)
	ERJSONDocumentTooDeep          = ErrorCode(3157)

	ERLockNowait                          = ErrorCode(3572)
	ERCTERecursiveRequiresUnion           = ErrorCode(3573)
	ERCTERecursiveForbidsAggregation      = ErrorCode(3575)
	ERCTERecursiveForbiddenJoinOrder      = ErrorCode(3576)
	ERCTERecursiveRequiresSingleReference = ErrorCode(3577)
	ERCTEMaxRecursionDepth                = ErrorCode(3636)
	ERRegexpStringNotTerminated           = ErrorCode(3684)
	ERRegexpBufferOverflow                = ErrorCode(3684)
	ERRegexpIllegalArgument               = ErrorCode(3685)
	ERRegexpIndexOutOfBounds              = ErrorCode(3686)
	ERRegexpInternal                      = ErrorCode(3687)
	ERRegexpRuleSyntax                    = ErrorCode(3688)
	ERRegexpBadEscapeSequence             = ErrorCode(3689)
	ERRegexpUnimplemented                 = ErrorCode(3690)
	ERRegexpMismatchParen                 = ErrorCode(3691)
	ERRegexpBadInterval                   = ErrorCode(3692)
	ERRRegexpMaxLtMin                     = ErrorCode(3693)
	ERRegexpInvalidBackRef                = ErrorCode(3694)
	ERRegexpLookBehindLimit               = ErrorCode(3695)
	ERRegexpMissingCloseBracket           = ErrorCode(3696)
	ERRegexpInvalidRange                  = ErrorCode(3697)
	ERRegexpStackOverflow                 = ErrorCode(3698)
	ERRegexpTimeOut                       = ErrorCode(3699)
	ERRegexpPatternTooBig                 = ErrorCode(3700)
	ERRegexpInvalidCaptureGroup           = ErrorCode(3887)
	ERRegexpInvalidFlag                   = ErrorCode(3900)

	ERCharacterSetMismatch = ErrorCode(3995)

	ERWrongParametersToNativeFct = ErrorCode(1583)

	// max execution time exceeded
	ERQueryTimeout = ErrorCode(3024)

	ErrCantCreateGeometryObject      = ErrorCode(1416)
	ErrGISDataWrongEndianess         = ErrorCode(3055)
	ErrNotImplementedForCartesianSRS = ErrorCode(3704)
	ErrNotImplementedForProjectedSRS = ErrorCode(3705)
	ErrNonPositiveRadius             = ErrorCode(3706)

	// server not available
	ERServerIsntAvailable = ErrorCode(3168)
)

// HandlerErrorCode is for errors thrown by the handler, and which are then embedded in other errors.
// See https://github.com/mysql/mysql-server/blob/trunk/include/my_base.h
type HandlerErrorCode uint16

func (e HandlerErrorCode) ToString() string {
	return strconv.FormatUint(uint64(e), 10)
}

const (
	// Didn't find key on read or update
	HaErrKeyNotFound = HandlerErrorCode(120)
	// Duplicate key on write
	HaErrFoundDuppKey = HandlerErrorCode(121)
	// Internal error
	HaErrInternalError = HandlerErrorCode(122)
	// Uppdate with is recoverable
	HaErrRecordChanged = HandlerErrorCode(123)
	// Wrong index given to function
	HaErrWrongIndex = HandlerErrorCode(124)
	// Transaction has been rolled back
	HaErrRolledBack = HandlerErrorCode(125)
	// Indexfile is crashed
	HaErrCrashed = HandlerErrorCode(126)
	// Record-file is crashed
	HaErrWrongInRecord = HandlerErrorCode(127)
	// Record-file is crashed
	HaErrOutOfMem = HandlerErrorCode(128)
	// not a MYI file - no signature
	HaErrNotATable = HandlerErrorCode(130)
	// Command not supported
	HaErrWrongCommand = HandlerErrorCode(131)
	// old database file
	HaErrOldFile = HandlerErrorCode(132)
	// No record read in update()
	HaErrNoActiveRecord = HandlerErrorCode(133)
	// A record is not there
	HaErrRecordDeleted = HandlerErrorCode(134)
	// No more room in file
	HaErrRecordFileFull = HandlerErrorCode(135)
	// No more room in file
	HaErrIndexFileFull = HandlerErrorCode(136)
	// end in next/prev/first/last
	HaErrEndOfFile = HandlerErrorCode(137)
	// unsupported extension used
	HaErrUnsupported = HandlerErrorCode(138)
	// Too big row
	HaErrTooBigRow = HandlerErrorCode(139)
	// Wrong create option
	HaWrongCreateOption = HandlerErrorCode(140)
	// Duplicate unique on write
	HaErrFoundDuppUnique = HandlerErrorCode(141)
	// Can't open charset
	HaErrUnknownCharset = HandlerErrorCode(142)
	// conflicting tables in MERGE
	HaErrWrongMrgTableDef = HandlerErrorCode(143)
	// Last (automatic?) repair failed
	HaErrCrashedOnRepair = HandlerErrorCode(144)
	// Table must be repaired
	HaErrCrashedOnUsage = HandlerErrorCode(145)
	// Lock wait timeout
	HaErrLockWaitTimeout = HandlerErrorCode(146)
	// Lock table is full
	HaErrLockTableFull = HandlerErrorCode(147)
	// Updates not allowed
	HaErrReadOnlyTransaction = HandlerErrorCode(148)
	// Deadlock found when trying to get lock
	HaErrLockDeadlock = HandlerErrorCode(149)
	// Cannot add a foreign key constr.
	HaErrCannotAddForeign = HandlerErrorCode(150)
	// Cannot add a child row
	HaErrNoReferencedRow = HandlerErrorCode(151)
	// Cannot delete a parent row
	HaErrRowIsReferenced = HandlerErrorCode(152)
	// No savepoint with that name
	HaErrNoSavepoint = HandlerErrorCode(153)
	// Non unique key block size
	HaErrNonUniqueBlockSize = HandlerErrorCode(154)
	// The table does not exist in engine
	HaErrNoSuchTable = HandlerErrorCode(155)
	// The table existed in storage engine
	HaErrTableExist = HandlerErrorCode(156)
	// Could not connect to storage engine
	HaErrNoConnection = HandlerErrorCode(157)
	// NULLs are not supported in spatial index
	HaErrNullInSpatial = HandlerErrorCode(158)
	// The table changed in storage engine
	HaErrTableDefChanged = HandlerErrorCode(159)
	// There's no partition in table for given value
	HaErrNoPartitionFound = HandlerErrorCode(160)
	// Row-based binlogging of row failed
	HaErrRbrLoggingFailed = HandlerErrorCode(161)
	// Index needed in foreign key constraint
	HaErrDropIndexFk = HandlerErrorCode(162)
	// Upholding foreign key constraints would lead to a duplicate key error in some other table.
	HaErrForeignDuplicateKey = HandlerErrorCode(163)
	// The table changed in storage engine
	HaErrTableNeedsUpgrade = HandlerErrorCode(164)
	// The table is not writable
	HaErrTableReadonly = HandlerErrorCode(165)
	// Failed to get next autoinc value
	HaErrAutoincReadFailed = HandlerErrorCode(166)
	// Failed to set row autoinc value
	HaErrAutoincErange = HandlerErrorCode(167)
	// Generic error
	HaErrGeneric = HandlerErrorCode(168)
	// row not actually updated: new values same as the old values
	HaErrRecordIsTheSame = HandlerErrorCode(169)
	// It is not possible to log this statement
	HaErrLoggingImpossible = HandlerErrorCode(170)
	// The event was corrupt, leading to illegal data being read
	HaErrCorruptEvent = HandlerErrorCode(171)
	// New file format
	HaErrNewFile = HandlerErrorCode(172)
	// The event could not be processed no other handler error happened
	HaErrRowsEventApply = HandlerErrorCode(173)
	// Error during initialization
	HaErrInitialization = HandlerErrorCode(174)
	// File too short
	HaErrFileTooShort = HandlerErrorCode(175)
	// Wrong CRC on page
	HaErrWrongCrc = HandlerErrorCode(176)
	// Too many active concurrent transactions
	HaErrTooManyConcurrentTrxs = HandlerErrorCode(177)
	// There's no explicitly listed partition in table for the given value
	HaErrNotInLockPartitions = HandlerErrorCode(178)
	// Index column length exceeds limit
	HaErrIndexColTooLong = HandlerErrorCode(179)
	// InnoDB index corrupted
	HaErrIndexCorrupt = HandlerErrorCode(180)
	// Undo log record too big
	HaErrUndoRecTooBig = HandlerErrorCode(181)
	// Invalid InnoDB Doc ID
	HaFtsInvalidDocid = HandlerErrorCode(182)
	// Table being used in foreign key check
	HaErrTableInFkCheck = HandlerErrorCode(183)
	// The tablespace existed in storage engine
	HaErrTablespaceExists = HandlerErrorCode(184)
	// Table has too many columns
	HaErrTooManyFields = HandlerErrorCode(185)
	// Row in wrong partition
	HaErrRowInWrongPartition = HandlerErrorCode(186)
	// InnoDB is in read only mode.
	HaErrInnodbReadOnly = HandlerErrorCode(187)
	// FTS query exceeds result cache limit
	HaErrFtsExceedResultCacheLimit = HandlerErrorCode(188)
	// Temporary file write failure
	HaErrTempFileWriteFailure = HandlerErrorCode(189)
	// Innodb is in force recovery mode
	HaErrInnodbForcedRecovery = HandlerErrorCode(190)
	// Too many words in a phrase
	HaErrFtsTooManyWordsInPhrase = HandlerErrorCode(191)
	// FK cascade depth exceeded
	HaErrFkDepthExceeded = HandlerErrorCode(192)
	// Option Missing during Create
	HaMissingCreateOption = HandlerErrorCode(193)
	// Out of memory in storage engine
	HaErrSeOutOfMemory = HandlerErrorCode(194)
	// Table/Clustered index is corrupted.
	HaErrTableCorrupt = HandlerErrorCode(195)
	// The query was interrupted
	HaErrQueryInterrupted = HandlerErrorCode(196)
	// Missing Tablespace
	HaErrTablespaceMissing = HandlerErrorCode(197)
	// Tablespace is not empty
	HaErrTablespaceIsNotEmpty = HandlerErrorCode(198)
	// Invalid Filename
	HaErrWrongFileName = HandlerErrorCode(199)
	// Operation is not allowed
	HaErrNotAllowedCommand = HandlerErrorCode(200)
	// Compute generated column value failed
	HaErrComputeFailed = HandlerErrorCode(201)
	// Table's row format has changed in the storage engine. Information in the data-dictionary needs to be updated.
	HaErrRowFormatChanged = HandlerErrorCode(202)
	// Don't wait for record lock
	HaErrNoWaitLock = HandlerErrorCode(203)
	// No more room in disk
	HaErrDiskFullNowait = HandlerErrorCode(204)
	// No session temporary space available
	HaErrNoSessionTemp = HandlerErrorCode(205)
	// Wrong or Invalid table name
	HaErrWrongTableName = HandlerErrorCode(206)
	// Path is too long for the OS
	HaErrTooLongPath = HandlerErrorCode(207)
	// Histogram sampling initialization failed
	HaErrSamplingInitFailed = HandlerErrorCode(208)
	// Too many sub-expression in search string
	HaErrFtsTooManyNestedExp = HandlerErrorCode(209)
)

// Sql states for errors.
// Originally found in include/mysql/sql_state.h
const (
	// SSUnknownSqlstate is ER_SIGNAL_EXCEPTION in
	// include/mysql/sql_state.h, but:
	// const char *unknown_sqlstate= "HY000"
	// in client.c. So using that one.
	SSUnknownSQLState = "HY000"

	// SSNetError is network related error
	SSNetError = "08S01"

	// SSWrongNumberOfColumns is related to columns error
	SSWrongNumberOfColumns = "21000"

	// SSWrongValueCountOnRow is related to columns count mismatch error
	SSWrongValueCountOnRow = "21S01"

	// SSDataTooLong is ER_DATA_TOO_LONG
	SSDataTooLong = "22001"

	// SSDataOutOfRange is ER_DATA_OUT_OF_RANGE
	SSDataOutOfRange = "22003"

	// SSConstraintViolation is constraint violation
	SSConstraintViolation = "23000"

	// SSCantDoThisDuringAnTransaction is
	// ER_CANT_DO_THIS_DURING_AN_TRANSACTION
	SSCantDoThisDuringAnTransaction = "25000"

	// SSAccessDeniedError is ER_ACCESS_DENIED_ERROR
	SSAccessDeniedError = "28000"

	// SSNoDB is ER_NO_DB_ERROR
	SSNoDB = "3D000"

	// SSLockDeadlock is ER_LOCK_DEADLOCK
	SSLockDeadlock = "40001"

	// SSClientError is the state on client errors
	SSClientError = "42000"

	// SSDupFieldName is ER_DUP_FIELD_NAME
	SSDupFieldName = "42S21"

	// SSBadFieldError is ER_BAD_FIELD_ERROR
	SSBadFieldError = "42S22"

	// SSUnknownTable is ER_UNKNOWN_TABLE
	SSUnknownTable = "42S02"

	// SSQueryInterrupted is ER_QUERY_INTERRUPTED;
	SSQueryInterrupted = "70100"
)

// IsConnErr returns true if the error is a connection error.
func IsConnErr(err error) bool {
	if IsTooManyConnectionsErr(err) {
		return false
	}
	if sqlErr, ok := err.(*SQLError); ok {
		num := sqlErr.Number()
		return (num >= CRUnknownError && num <= CRNamedPipeStateError) || num == ERQueryInterrupted
	}
	return false
}

// IsConnLostDuringQuery returns true if the error is a CRServerLost error.
// Happens most commonly when a query is killed MySQL server-side.
func IsConnLostDuringQuery(err error) bool {
	if sqlErr, ok := err.(*SQLError); ok {
		num := sqlErr.Number()
		return (num == CRServerLost)
	}
	return false
}

// IsEphemeralError returns true if the error is ephemeral and the caller should
// retry if possible. Note: non-SQL errors are always treated as ephemeral.
func IsEphemeralError(err error) bool {
	if sqlErr, ok := err.(*SQLError); ok {
		en := sqlErr.Number()
		switch en {
		case
			CRConnectionError,
			CRConnHostError,
			CRMalformedPacket,
			CRNamedPipeStateError,
			CRServerHandshakeErr,
			CRServerGone,
			CRServerLost,
			CRSSLConnectionError,
			CRUnknownError,
			CRUnknownHost,
			ERCantCreateThread,
			ERDiskFull,
			ERForcingClose,
			ERGotSignal,
			ERHostIsBlocked,
			ERLockTableFull,
			ERInnodbReadOnly,
			ERInternalError,
			ERLockDeadlock,
			ERLockWaitTimeout,
			ERQueryTimeout,
			EROutOfMemory,
			EROutOfResources,
			EROutOfSortMemory,
			ERQueryInterrupted,
			ERServerIsntAvailable,
			ERServerShutdown,
			ERTooManyUserConnections,
			ERUnknownError,
			ERUserLimitReached:
			return true
		default:
			return false
		}
	}
	// If it's not an sqlError then we assume it's ephemeral
	return true
}

// IsTooManyConnectionsErr returns true if the error is due to too many connections.
func IsTooManyConnectionsErr(err error) bool {
	if sqlErr, ok := err.(*SQLError); ok {
		if sqlErr.Number() == CRServerHandshakeErr && strings.Contains(sqlErr.Message, "Too many connections") {
			return true
		}
	}
	return false
}

// IsSchemaApplyError returns true when given error is a MySQL error applying schema change
func IsSchemaApplyError(err error) bool {
	merr, isSQLErr := err.(*SQLError)
	if !isSQLErr {
		return false
	}
	switch merr.Num {
	case
		ERDupKeyName,
		ERCantDropFieldOrKey,
		ERTableExists,
		ERDupFieldName:
		return true
	}
	return false
}

// Error codes for client-side errors.
// Originally found in include/mysql/errmsg.h and
// https://dev.mysql.com/doc/mysql-errors/en/client-error-reference.html
const (
	// CRUnknownError is CR_UNKNOWN_ERROR
	CRUnknownError = ErrorCode(2000)

	// CRConnectionError is CR_CONNECTION_ERROR
	// This is returned if a connection via a Unix socket fails.
	CRConnectionError = ErrorCode(2002)

	// CRConnHostError is CR_CONN_HOST_ERROR
	// This is returned if a connection via a TCP socket fails.
	CRConnHostError = ErrorCode(2003)

	// CRUnknownHost is CR_UNKNOWN_HOST
	// This is returned if the host name cannot be resolved.
	CRUnknownHost = ErrorCode(2005)

	// CRServerGone is CR_SERVER_GONE_ERROR.
	// This is returned if the client tries to send a command but it fails.
	CRServerGone = ErrorCode(2006)

	// CRVersionError is CR_VERSION_ERROR
	// This is returned if the server versions don't match what we support.
	CRVersionError = ErrorCode(2007)

	// CRServerHandshakeErr is CR_SERVER_HANDSHAKE_ERR
	CRServerHandshakeErr = ErrorCode(2012)

	// CRServerLost is CR_SERVER_LOST.
	// Used when:
	// - the client cannot write an initial auth packet.
	// - the client cannot read an initial auth packet.
	// - the client cannot read a response from the server.
	//     This happens when a running query is killed.
	CRServerLost = ErrorCode(2013)

	// CRCommandsOutOfSync is CR_COMMANDS_OUT_OF_SYNC
	// Sent when the streaming calls are not done in the right order.
	CRCommandsOutOfSync = ErrorCode(2014)

	// CRNamedPipeStateError is CR_NAMEDPIPESETSTATE_ERROR.
	// This is the highest possible number for a connection error.
	CRNamedPipeStateError = ErrorCode(2018)

	// CRCantReadCharset is CR_CANT_READ_CHARSET
	CRCantReadCharset = ErrorCode(2019)

	// CRSSLConnectionError is CR_SSL_CONNECTION_ERROR
	CRSSLConnectionError = ErrorCode(2026)

	// CRMalformedPacket is CR_MALFORMED_PACKET
	CRMalformedPacket = ErrorCode(2027)
)
