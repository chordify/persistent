{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

{-# OPTIONS_GHC -fno-warn-deprecations #-} -- Pattern match 'PersistDbSpecific'
-- | A MySQL backend for @persistent@.
module Database.Persist.MySQL
  ( withMySQLPool
  , withMySQLConn
  , createMySQLPool
  , module Database.Persist.Sql
  , MySQLConnectInfo
  , MySQL.defaultConnectInfo
  , MySQL.ConnectInfo(..)
  , mkMySQLConnectInfo
  , setMySQLConnectInfoPort
  , setMySQLConnectInfoCharset
  , MySQLConf
  , mkMySQLConf
  , mockMigration
  -- * @ON DUPLICATE KEY UPDATE@ Functionality
  , insertOnDuplicateKeyUpdate
  , insertEntityOnDuplicateKeyUpdate
  , insertManyOnDuplicateKeyUpdate
  , insertEntityManyOnDuplicateKeyUpdate
  , HandleUpdateCollision
  , copyField
  , copyUnlessNull
  , copyUnlessEmpty
  , copyUnlessEq
  , copyWhenGreater
  -- * TLS configuration
  , setMySQLConnectInfoTLS
  , MySQLTLS.TrustedCAStore(..)
  , MySQLTLS.makeClientParams
  , MySQLTLS.makeClientParams'
  , openMySQLConn
  -- * persistent-mysql compatibility
  , myConnInfo
  , myPoolSize
) where

import Control.Arrow
import Control.Monad
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Control.Monad.Logger (MonadLoggerIO, runNoLoggingT)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT, runExceptT)
import Control.Monad.Trans.Reader (runReaderT, ReaderT)
import Control.Monad.Trans.Writer (runWriterT)

import qualified Data.List.NonEmpty as NEL
import Data.Acquire (Acquire, mkAcquire, with)
import Data.Aeson
import Data.Aeson.Types (modifyFailure)
import qualified Data.ByteString.Lazy as BS
import qualified Data.ByteString.Char8  as BSC
import           Data.Conduit (ConduitM, (.|), runConduit, runConduitRes)
import qualified Data.Conduit.List as CL
import Data.Either (partitionEithers)
import Data.Fixed (Pico)
import Data.Function (on)
import Data.IORef
import Data.Int (Int64)
import Data.List (find, groupBy, intercalate, sort)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, listToMaybe, mapMaybe)
import qualified Data.Monoid as Monoid
import Data.Pool (Pool)
import Data.Text (Text, pack)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import GHC.Stack
import System.Environment (getEnvironment)

import Database.Persist.Sql
import qualified Database.Persist.SqlBackend as SqlBackend
import Database.Persist.SqlBackend.Internal
import Database.Persist.SqlBackend.StatementCache (mkSimpleStatementCache, mkStatementCache)
import Database.Persist.Sql.Types.Internal (mkPersistBackend, makeIsolationLevelStatement)
import qualified Database.Persist.Sql.Util as Util
import Database.Persist.MySQLConnectInfoShowInstance ()

import qualified Database.MySQL.Base    as MySQL
import qualified Database.MySQL.Protocol.Escape as MySQL
import qualified Database.MySQL.TLS     as MySQLTLS
import qualified Network.TLS            as TLS
import qualified System.IO.Streams      as Streams
import qualified Data.Time.Calendar     as Time
import qualified Data.Time.LocalTime    as Time
import qualified Network.Socket         as NetworkSocket
import qualified Data.Word              as Word
import           Data.String (fromString)

-- | Create a MySQL connection pool and run the given action.
-- The pool is properly released after the action finishes using
-- it.  Note that you should not use the given 'ConnectionPool'
-- outside the action since it may be already been released.
withMySQLPool :: (MonadLoggerIO m, MonadUnliftIO m, IsPersistBackend backend, BaseBackend backend ~ SqlBackend, BackendCompatible SqlBackend backend)
    => MySQLConnectInfo
    -- ^ Connection information.
    -> Int
    -- ^ Number of connections to be kept open in the pool.
    -> (Pool backend -> m a)
    -- ^ Action to be executed that uses the connection pool.
    -> m a
withMySQLPool ci = withSqlPool $ open' ci

-- | Create a MySQL connection pool.  Note that it's your
-- responsibility to properly close the connection pool when
-- unneeded.  Use 'withMySQLPool' for automatic resource control.
createMySQLPool :: (MonadUnliftIO m, MonadLoggerIO m, IsPersistBackend backend, BaseBackend backend ~ SqlBackend, BackendCompatible SqlBackend backend)
    => MySQLConnectInfo
    -- ^ Connection information.
    -> Int
    -- ^ Number of connections to be kept open in the pool.
    -> m (Pool backend)
createMySQLPool ci = createSqlPool $ open' ci

-- | Same as 'withMySQLPool', but instead of opening a pool
-- of connections, only one connection is opened.
withMySQLConn :: (MonadUnliftIO m, MonadLoggerIO m, IsPersistBackend backend, BaseBackend backend ~ SqlBackend, BackendCompatible SqlBackend backend)
    => MySQLConnectInfo
    -- ^ Connection information.
    -> (backend -> m a)
    -- ^ Action to be executed that uses the connection.
    -> m a
withMySQLConn = withSqlConn . open'

-- | Internal function that opens a @mysql-haskell@ connection to the server.
connect' :: MySQLConnectInfo -> IO MySQL.MySQLConn
connect' (MySQLConnectInfo innerCi Nothing)
  = MySQL.connect innerCi
connect' (MySQLConnectInfo innerCi (Just tls))
  = MySQLTLS.connect innerCi (tls, "persistent-mysql-haskell")

-- | Open a connection to MySQL server, initialize the 'SqlBackend' and return
-- their tuple
--
-- @since 2.12.1.0
openMySQLConn :: (IsPersistBackend backend, BaseBackend backend ~ SqlBackend)
      => MySQLConnectInfo
      -> LogFunc
      -> IO (MySQL.MySQLConn, backend)
openMySQLConn ci@(MySQLConnectInfo innerCi _) logFunc = do
    conn <- connect' ci
    autocommit' conn False -- disable autocommit!
    smap <- newIORef $ Map.empty
    let stCache = mkStatementCache $ mkSimpleStatementCache smap
    let backend =
          mkPersistBackend $
          projectBackend $
          SqlBackend
          { connPrepare    = prepare' conn
          , connStmtMap    = stCache
          , connInsertSql  = insertSql'
          , connInsertManySql = Nothing
          , connUpsertSql = Nothing
          , connPutManySql = Just putManySql
          , connClose      = MySQL.close conn
          , connMigrateSql = migrate' innerCi
          , connBegin      = const $ begin' conn
          , connCommit     = const $ commit' conn
          , connRollback   = const $ rollback' conn
          , connEscapeFieldName = T.pack . escapeF
          , connEscapeTableName = T.pack . escapeE . getEntityDBName
          , connEscapeRawName = T.pack . escapeDBName . T.unpack
          , connNoLimit    = "LIMIT 18446744073709551615"
          -- This noLimit is suggested by MySQL's own docs, see
          -- <http://dev.mysql.com/doc/refman/5.5/en/select.html>
          , connRDBMS      = "mysql"
          , connLimitOffset = decorateSQLWithLimitOffset "LIMIT 18446744073709551615"
          , connLogFunc    = logFunc
          , connMaxParams = Nothing
          , connRepsertManySql = Just repsertManySql
          , connVault = mempty
          , connHooks = emptySqlBackendHooks
          }
    pure (conn,backend)

-- | Internal function that opens a connection to the MySQL server.
open' :: (IsPersistBackend backend, BaseBackend backend ~ SqlBackend)
      => MySQLConnectInfo
      -> LogFunc
      -> IO backend
open' ci logFunc = snd <$> openMySQLConn ci logFunc

-- | Set autocommit setting
autocommit' :: MySQL.MySQLConn -> Bool -> IO ()
autocommit' conn bool = void $ MySQL.execute conn "SET autocommit=?" [encodeBool bool]

-- | Start a transaction.
begin' :: MySQL.MySQLConn -> Maybe IsolationLevel -> IO ()
begin' conn mIso
  = void
  $ mapM_ (MySQL.execute_ conn . fromString . makeIsolationLevelStatement) mIso
  >> MySQL.execute_ conn "BEGIN"

-- | Commit the current transaction.
commit' :: MySQL.MySQLConn -> IO ()
commit' conn = void $ MySQL.execute_ conn "COMMIT"

-- | Rollback the current transaction.
rollback' :: MySQL.MySQLConn -> IO ()
rollback' conn = void $ MySQL.execute_ conn "ROLLBACK"

-- | Prepare a query.  We don't support prepared statements, but
-- we'll do some client-side preprocessing here.
prepare' :: MySQL.MySQLConn -> Text -> IO Statement
prepare' conn sql = do
    let query = MySQL.Query . BS.fromStrict . T.encodeUtf8 $ sql
    return Statement
        { stmtFinalize = return ()
        , stmtReset    = return ()
        , stmtExecute  = execute' conn query
        , stmtQuery    = withStmt' conn query
        }


-- | SQL code to be executed when inserting an entity.
insertSql' :: EntityDef -> [PersistValue] -> InsertSqlResult
insertSql' ent vals =
    case getEntityId ent of
        EntityIdNaturalKey _ ->
            ISRManyKeys sql vals
        EntityIdField _ ->
            ISRInsertGet sql "SELECT LAST_INSERT_ID()"
  where
    (fieldNames, placeholders) = unzip (Util.mkInsertPlaceholders ent escapeFT)
    sql = T.concat
        [ "INSERT INTO "
        , escapeET $ getEntityDBName ent
        , "("
        , T.intercalate "," fieldNames
        , ") VALUES("
        , T.intercalate "," placeholders
        , ")"
        ]

-- | Execute an statement that doesn't return any results.
execute' :: MySQL.MySQLConn -> MySQL.Query -> [PersistValue] -> IO Int64
execute' conn query vals
  = fmap (fromIntegral . MySQL.okAffectedRows) $ MySQL.execute conn query (map P vals)

-- | query' allows arguments to be empty.
query'
  :: MySQL.QueryParam p => MySQL.MySQLConn -> MySQL.Query -> [p]
  -> IO ([MySQL.ColumnDef], Streams.InputStream [MySQL.MySQLValue])
query' conn qry [] = MySQL.query_ conn qry
query' conn qry ps = MySQL.query  conn qry ps

-- | Execute an statement that does return results.
-- unlike @persistent-mysql@, we actually _stream_ results.
withStmt' :: MonadIO m
          => MySQL.MySQLConn
          -> MySQL.Query
          -> [PersistValue]
          -> Acquire (ConduitM () [PersistValue] m ())
withStmt' conn query vals
  = fetchRows <$> mkAcquire createResult releaseResult
  where
    createResult = query' conn query (map P vals)
    releaseResult (_, is) = Streams.skipToEof is
    fetchRows (fields, is) = CL.unfoldM getVal is
      where
      -- Find out the type of the columns
          getters = fmap getGetter fields
          convert = zipWith (\g -> \c -> g c) getters
          getVal s = do
            v <- liftIO $ Streams.read s
            case v of
              (Just r)  -> pure $ Just (convert r, s)
              _         -> pure Nothing

-- | Encode a Haskell bool into a MySQLValue
encodeBool :: Bool -> MySQL.MySQLValue
encodeBool True = MySQL.MySQLInt8U 1
encodeBool False = MySQL.MySQLInt8U 0

-- | Decode a whole number into a PersistInt64
decodeInteger :: Integral a => a -> PersistValue
decodeInteger = PersistInt64 . fromIntegral

-- | Decode a decimal number into a PersistDouble
decodeDouble :: Real a => a -> PersistValue
decodeDouble = PersistDouble . realToFrac

-- | @newtype@ around 'PersistValue' that supports the
-- 'MySQL.Param' type class.
newtype P = P PersistValue

instance MySQL.QueryParam P where
  render (P (PersistText t))        = MySQL.putTextField $ MySQL.MySQLText t
  render (P (PersistByteString b))  = MySQL.putTextField $ MySQL.MySQLBytes b
  render (P (PersistInt64 i))       = MySQL.putTextField $ MySQL.MySQLInt64 i
  render (P (PersistDouble d))      = MySQL.putTextField $ MySQL.MySQLDouble d
  render (P (PersistBool b))        = MySQL.putTextField $ encodeBool b
  render (P (PersistDay d))         = MySQL.putTextField $ MySQL.MySQLDate d
  render (P (PersistTimeOfDay t))   = MySQL.putTextField $ MySQL.MySQLTime 0 t
  render (P (PersistUTCTime t))     = MySQL.putTextField . MySQL.MySQLTimeStamp $ Time.utcToLocalTime Time.utc t
  render (P (PersistNull))          = MySQL.putTextField $ MySQL.MySQLNull
  render (P (PersistList l))        = MySQL.putTextField . MySQL.MySQLText $ listToJSON l
  render (P (PersistMap m))         = MySQL.putTextField . MySQL.MySQLText $ mapToJSON m
  render (P (PersistRational r))    =
    MySQL.putTextField $ MySQL.MySQLDecimal $ read $ show (fromRational r :: Pico)
    -- FIXME: Too Ambigous, can not select precision without information about field
  render (P (PersistLiteral_ DbSpecific s)) = MySQL.putTextField $ MySQL.MySQLBytes s
  render (P (PersistLiteral_ Unescaped l)) = MySQL.putTextField $ MySQL.MySQLBytes l
  render (P (PersistLiteral_ Escaped e)) = MySQL.putTextField $ MySQL.MySQLBytes $ MySQL.escapeBytes e
  render (P (PersistArray a))       = MySQL.render (P (PersistList a))
  render (P (PersistObjectId _))    =
    error "Refusing to serialize a PersistObjectId to a MySQL value"

-- | @Getter a@ is a function that converts an incoming "MySQLValue"
-- into a data type @a@.
type Getter a = MySQL.MySQLValue -> a

-- | Get the corresponding @'Getter' 'PersistValue'@ depending on
-- the type of the column.
getGetter :: MySQL.ColumnDef -> Getter PersistValue
getGetter _field = go
  where
    -- Int64
    go (MySQL.MySQLInt8U  v) = decodeInteger v
    go (MySQL.MySQLInt8   v) = decodeInteger v
    go (MySQL.MySQLInt16U v) = decodeInteger v
    go (MySQL.MySQLInt16  v) = decodeInteger v
    go (MySQL.MySQLInt32U v) = decodeInteger v
    go (MySQL.MySQLInt32  v) = decodeInteger v
    go (MySQL.MySQLInt64U v) = decodeInteger v
    go (MySQL.MySQLInt64  v) = decodeInteger v
    go (MySQL.MySQLBit    v) = decodeInteger v
    -- Double
    -- TODO: FIX WARNING(S) AND TRY TO PROVIDE LEAST PRECISION LOSS
    go (MySQL.MySQLFloat    v) = decodeDouble v
    go (MySQL.MySQLDouble   v) = decodeDouble v
    go (MySQL.MySQLDecimal  v) = decodeDouble v
    -- ByteString and Text
    go (MySQL.MySQLBytes  v) = PersistByteString v
    go (MySQL.MySQLText   v) = PersistText v
    -- Time-related
    -- TODO: REMOVE ASSUMPTION THAT DATETIME and TIMESTAMP are in UTC
    go (MySQL.MySQLDateTime   v) = PersistUTCTime $ Time.localTimeToUTC Time.utc v
    go (MySQL.MySQLTimeStamp  v) = PersistUTCTime $ Time.localTimeToUTC Time.utc v
    go (MySQL.MySQLYear       v) = PersistDay (Time.fromGregorian (fromIntegral v) 1 1)
    go (MySQL.MySQLDate       v) = PersistDay v
    go (MySQL.MySQLTime _     v) = PersistTimeOfDay v
    -- Null
    go (MySQL.MySQLNull        ) = PersistNull
    -- Conversion using PersistDbSpecific
    go (MySQL.MySQLGeometry   v) = PersistLiteral v

----------------------------------------------------------------------


-- | Create the migration plan for the given 'PersistEntity'
-- @val@.
migrate' :: MySQL.ConnectInfo
         -> [EntityDef]
         -> (Text -> IO Statement)
         -> EntityDef
         -> IO (Either [Text] [(Bool, Text)])
migrate' connectInfo allDefs getter val = do
    let name = getEntityDBName val
    let (newcols, udefs, fdefs) = mysqlMkColumns allDefs val
    old <- getColumns connectInfo getter val newcols
    let udspair = map udToPair udefs
    case ([], old, partitionEithers old) of
        -- Nothing found, create everything
        ([], [], _) -> do
            let uniques = do
                    (uname, ucols) <- udspair
                    pure
                        $ AlterTable name
                        $ AddUniqueConstraint uname
                        $ map (findTypeAndMaxLen name) ucols

            let foreigns = do
                    Column { cName=cname, cReference=Just cRef } <- newcols
                    let refConstraintName = crConstraintName cRef
                    let refTblName = crTableName cRef
                    let refTarget =
                          addReference allDefs refConstraintName refTblName cname (crFieldCascade cRef)

                    guard $ Just cname /= fmap fieldDB (getEntityIdField val)
                    return $ AlterColumn name refTarget


            let foreignsAlt =
                    map
                        (\fdef ->
                            let (childfields, parentfields) =
                                    unzip
                                    $ map (\((_,b),(_,d)) -> (b,d))
                                    $ foreignFields fdef
                            in
                                AlterColumn
                                    name
                                    (AddReference
                                        (foreignRefTableDBName fdef)
                                        (foreignConstraintNameDBName fdef)
                                        childfields
                                        parentfields
                                        (foreignFieldCascade fdef)
                                    )
                        )
                        fdefs

            return
                $ Right
                $ map showAlterDb
                $ (addTable newcols val) : uniques ++ foreigns ++ foreignsAlt

        -- No errors and something found, migrate
        (_, _, ([], old')) -> do
            let excludeForeignKeys (xs,ys) =
                    ( map
                        (\c ->
                            case cReference c of
                                Just ColumnReference {crConstraintName=fk} ->
                                    case find (\f -> fk == foreignConstraintNameDBName f) fdefs of
                                        Just _ -> c { cReference = Nothing }
                                        Nothing -> c
                                Nothing -> c
                        )
                        xs
                    , ys
                    )
                (acs, ats) =
                    getAlters
                        allDefs
                        val
                        (newcols, udspair)
                        $ excludeForeignKeys
                        $ partitionEithers
                        $ old'
                acs' =
                    map (AlterColumn name) acs
                ats' =
                    map (AlterTable  name) ats
            return
                $ Right
                $ map showAlterDb
                $ acs' ++ ats'

        -- Errors
        (_, _, (errs, _)) ->
            return $ Left errs

      where
        findTypeAndMaxLen tblName col =
            let (col', ty) = findTypeOfColumn allDefs tblName col
                (_, ml) = findMaxLenOfColumn allDefs tblName col
            in
                (col', ty, ml)

addTable :: [Column] -> EntityDef -> AlterDB
addTable cols entity = AddTable $ concat
    -- Lower case e: see Database.Persist.Sql.Migration
    [ "CREATe TABLE "
    , escapeE name
    , "("
    , idtxt
    , if null nonIdCols then [] else ","
    , intercalate "," $ map showColumn nonIdCols
    , ")"
    ]
  where
    nonIdCols =
        filter (\c -> Just (cName c) /= fmap fieldDB (getEntityIdField entity) ) cols
    name =
        getEntityDBName entity
    idtxt =
        case getEntityId entity of
            EntityIdNaturalKey pdef ->
                concat
                    [ " PRIMARY KEY ("
                    , intercalate ","
                  $ map (escapeF . fieldDB) $ NEL.toList $ compositeFields pdef
                    , ")"
                    ]
            EntityIdField idField ->
                let
                    defText =
                        defaultAttribute $ fieldAttrs idField
                    sType =
                        fieldSqlType idField
                    autoIncrementText =
                        case (sType, defText) of
                            (SqlInt64, Nothing) -> " AUTO_INCREMENT"
                            _ -> ""
                    maxlen =
                        findMaxLenOfField idField
                in
                    concat
                        [ escapeF $ fieldDB idField
                        , " " <> showSqlType sType maxlen False
                        , " NOT NULL"
                        , autoIncrementText
                        , " PRIMARY KEY"
                        , case defText of
                            Nothing ->
                                ""
                            Just def ->
                                concat
                                    [ " DEFAULT ("
                                    , T.unpack def
                                    , ")"
                                    ]
                        ]

-- | Find out the type of a column.
findTypeOfColumn :: [EntityDef] -> EntityNameDB -> FieldNameDB -> (FieldNameDB, FieldType)
findTypeOfColumn allDefs name col =
    maybe
        (error $ "Could not find type of column " ++
                   show col ++ " on table " ++ show name ++
                   " (allDefs = " ++ show allDefs ++ ")"
        )
        ((,) col)
        $ do
            entDef   <- find ((== name) . getEntityDBName) allDefs
            fieldDef <- find ((== col)  . fieldDB) (getEntityFieldsDatabase entDef)
            return (fieldType fieldDef)

-- | Find out the maxlen of a column (default to 200)
findMaxLenOfColumn :: [EntityDef] -> EntityNameDB -> FieldNameDB -> (FieldNameDB, Integer)
findMaxLenOfColumn allDefs name col =
   maybe (col, 200)
         ((,) col) $ do
           entDef     <- find ((== name) . getEntityDBName) allDefs
           fieldDef   <- find ((== col) . fieldDB) (getEntityFieldsDatabase entDef)
           findMaxLenOfField fieldDef

-- | Find out the maxlen of a field
findMaxLenOfField :: FieldDef -> Maybe Integer
findMaxLenOfField fieldDef =
    listToMaybe
        . mapMaybe (\case
            FieldAttrMaxlen x -> Just x
            _ -> Nothing)
        . fieldAttrs
        $ fieldDef

-- | Helper for 'AddReference' that finds out the which primary key columns to reference.
addReference
    :: [EntityDef]
    -- ^ List of all known 'EntityDef's.
    -> ConstraintNameDB
    -- ^ Foreign key name
    -> EntityNameDB
    -- ^ Referenced table name
    -> FieldNameDB
    -- ^ Column name
    -> FieldCascade
    -> AlterColumn
addReference allDefs fkeyname reftable cname fc =
    AddReference reftable fkeyname [cname] referencedColumns fc
  where
    errorMessage =
        error
            $ "Could not find ID of entity " ++ show reftable
            ++ " (allDefs = " ++ show allDefs ++ ")"
    referencedColumns =
        fromMaybe errorMessage $ do
            entDef <- find ((== reftable) . getEntityDBName) allDefs
            return $ map fieldDB $ NEL.toList $ getEntityKeyFields entDef

data AlterColumn = Change Column
                 | Add' Column
                 | Drop Column
                 | Default Column String
                 | NoDefault Column
                 | Gen Column SqlType (Maybe Integer) String
                 | NoGen Column SqlType (Maybe Integer)
                 | Update' Column String
                 -- | See the definition of the 'showAlter' function to see how these fields are used.
                 | AddReference
                    EntityNameDB -- Referenced table
                    ConstraintNameDB -- Foreign key name
                    [FieldNameDB] -- Referencing columns
                    [FieldNameDB] -- Referenced columns
                    FieldCascade
                 | DropReference ConstraintNameDB
                 deriving Show

data AlterTable = AddUniqueConstraint ConstraintNameDB [(FieldNameDB, FieldType, Integer)]
                | DropUniqueConstraint ConstraintNameDB
                deriving Show

data AlterDB = AddTable String
             | AlterColumn EntityNameDB AlterColumn
             | AlterTable EntityNameDB AlterTable
             deriving Show


udToPair :: UniqueDef -> (ConstraintNameDB, [FieldNameDB])
udToPair ud = (uniqueDBName ud, map snd $ NEL.toList $ uniqueFields ud)

----------------------------------------------------------------------


-- | Returns all of the 'Column'@s@ in the given table currently
-- in the database.
getColumns
    :: HasCallStack
    => MySQL.ConnectInfo
    -> (Text -> IO Statement)
    -> EntityDef -> [Column]
    -> IO [Either Text (Either Column (ConstraintNameDB, [FieldNameDB]))]
getColumns connectInfo getter def cols = do

    -- Find out all columns.
    stmtClmns <- getter $ T.concat
      [ "SELECT COLUMN_NAME, "
      ,   "IS_NULLABLE, "
      ,   "DATA_TYPE, "
      ,   "COLUMN_TYPE, "
      ,   "CHARACTER_MAXIMUM_LENGTH, "
      ,   "NUMERIC_PRECISION, "
      ,   "NUMERIC_SCALE, "
      ,   "COLUMN_DEFAULT, "
      ,   "GENERATION_EXPRESSION "
      , "FROM INFORMATION_SCHEMA.COLUMNS "
      , "WHERE TABLE_SCHEMA = ? "
      ,   "AND TABLE_NAME   = ? "
      -- ,   "AND COLUMN_NAME <> ?"
      ]
    inter2 <- with (stmtQuery stmtClmns vals) (\src -> runConduitRes $ src .| CL.consume)
    cs <- runConduitRes $ CL.sourceList inter2 .| helperClmns -- avoid nested queries

    -- Find out the constraints.
    stmtCntrs <- getter $ T.concat
      [ "SELECT CONSTRAINT_NAME, "
      ,   "COLUMN_NAME "
      , "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
      , "WHERE TABLE_SCHEMA = ? "
      ,   "AND TABLE_NAME   = ? "
      -- ,   "AND COLUMN_NAME <> ? "
      ,   "AND CONSTRAINT_NAME <> 'PRIMARY' "
      ,   "AND REFERENCED_TABLE_SCHEMA IS NULL "
      , "ORDER BY CONSTRAINT_NAME, "
      ,   "COLUMN_NAME"
      ]
    us <- with (stmtQuery stmtCntrs vals) (\src -> runConduitRes $ src .| helperCntrs)

    -- Return both
    return (cs ++ us)
  where
    refMap = Map.fromList $ foldl ref [] cols
      where ref rs c = case cReference c of
                Nothing -> rs
                (Just r) -> (unFieldNameDB $ cName c, r) : rs
    vals = [ PersistText $ T.decodeUtf8 $ MySQL.ciDatabase connectInfo
           , PersistText $ unEntityNameDB $ getEntityDBName def
        --   , PersistText $ unDBName $ fieldDB $ getEntityId def
           ]

    helperClmns = CL.mapM getIt .| CL.consume
        where
          getIt row = fmap (either Left (Right . Left)) .
                      liftIO .
                      getColumn connectInfo getter (getEntityDBName def) row $ ref
            where ref = case row of
                    (PersistText cname : _) -> (Map.lookup cname refMap)
                    _ -> Nothing

    helperCntrs = do
      let check [ PersistText cntrName
                , PersistText clmnName] = return ( cntrName, clmnName )
          check other = fail $ "helperCntrs: unexpected " ++ show other
      rows <- mapM check =<< CL.consume
      return $ map (Right . Right . (ConstraintNameDB . fst . head &&& map (FieldNameDB . snd)))
             $ groupBy ((==) `on` fst) rows


-- | Get the information about a column in a table.
getColumn
    :: HasCallStack
    => MySQL.ConnectInfo
    -> (Text -> IO Statement)
    -> EntityNameDB
    -> [PersistValue]
    -> Maybe ColumnReference
    -> IO (Either Text Column)
getColumn connectInfo getter tname [ PersistText cname
                                   , PersistText null_
                                   , PersistText dataType
                                   , PersistText colType
                                   , colMaxLen
                                   , colPrecision
                                   , colScale
                                   , default'
                                   , generated
                                   ] cRef =
    fmap (either (Left . pack) Right) $
    runExceptT $ do
        -- Default value
        default_ <-
            case default' of
                PersistNull -> return Nothing
                PersistText t -> return (Just t)
                PersistByteString bs ->
                    case T.decodeUtf8' bs of
                        Left exc ->
                            fail
                                $ "Invalid default column: "
                                ++ show default'
                                ++ " (error: " ++ show exc ++ ")"
                        Right t ->
                            return (Just t)
                _ ->
                    fail $ "Invalid default column: " ++ show default'

        generated_ <-
            case generated of
                PersistNull -> return Nothing
                PersistText "" -> return Nothing
                PersistByteString "" -> return Nothing
                PersistText t -> return (Just t)
                PersistByteString bs ->
                    case T.decodeUtf8' bs of
                        Left exc ->
                            fail
                                $ "Invalid generated column: "
                                ++ show generated
                                ++ " (error: " ++ show exc ++ ")"
                        Right t ->
                            return (Just t)
                _ ->
                    fail $ "Invalid generated column: " ++ show generated

        ref <- getRef (crConstraintName <$> cRef)

        let colMaxLen' =
                case colMaxLen of
                    PersistInt64 l -> Just (fromIntegral l)
                    _ -> Nothing
            ci = ColumnInfo
              { ciColumnType = colType
              , ciMaxLength = colMaxLen'
              , ciNumericPrecision = colPrecision
              , ciNumericScale = colScale
              }

        (typ, maxLen) <- parseColumnType dataType ci

        -- Okay!
        return Column
            { cName = FieldNameDB cname
            , cNull = null_ == "YES"
            , cSqlType = typ
            , cDefault = default_
            , cGenerated = generated_
            , cDefaultConstraintName = Nothing
            , cMaxLen = maxLen
            , cReference = ref
            }
  where
    getRef Nothing = return Nothing
    getRef (Just refName') = do
        -- Foreign key (if any)
        stmt <- lift . getter $ T.concat
            [ "SELECT KCU.REFERENCED_TABLE_NAME, "
            ,   "KCU.CONSTRAINT_NAME, "
            ,   "KCU.ORDINAL_POSITION, "
            ,   "DELETE_RULE, "
            ,   "UPDATE_RULE "
            , "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU "
            , "INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC "
            , "  USING (CONSTRAINT_SCHEMA, CONSTRAINT_NAME) "
            , "WHERE KCU.TABLE_SCHEMA = ? "
            ,   "AND KCU.TABLE_NAME   = ? "
            ,   "AND KCU.COLUMN_NAME  = ? "
            ,   "AND KCU.REFERENCED_TABLE_SCHEMA = ? "
            ,   "AND KCU.CONSTRAINT_NAME = ? "
            , "ORDER BY KCU.CONSTRAINT_NAME, "
            ,   "KCU.COLUMN_NAME"
            ]
        let vars =
                [ PersistText $ T.decodeUtf8 $ MySQL.ciDatabase connectInfo
                , PersistText $ unEntityNameDB tname
                , PersistText cname
                , PersistText $ T.decodeUtf8 $ MySQL.ciDatabase connectInfo
                , PersistText $ unConstraintNameDB refName'
                ]
            parseCascadeAction txt =
                case txt of
                    "RESTRICT" -> Just Restrict
                    "CASCADE" -> Just Cascade
                    "SET NULL" -> Just SetNull
                    "SET DEFAULT" -> Just SetDefault
                    "NO ACTION" -> Nothing
                    _ ->
                        error $ "Unexpected value in parseCascadeAction: " <> show txt

        cntrs <- liftIO $ with (stmtQuery stmt vars) (\src -> runConduit $ src .| CL.consume)
        pure $ case cntrs of
            [] ->
                Nothing
            [[PersistText tab, PersistText ref, PersistInt64 pos, PersistText onDel, PersistText onUpd]] ->
                if pos == 1
                then Just $ ColumnReference (EntityNameDB tab) (ConstraintNameDB ref) FieldCascade
                    { fcOnUpdate = parseCascadeAction onUpd
                    , fcOnDelete = parseCascadeAction onDel
                    }
                else Nothing
            xs -> error $ mconcat
              [ "MySQL.getColumn/getRef: error fetching constraints. Expected a single result for foreign key query for table: "
              , T.unpack (unEntityNameDB tname)
              , " and column: "
              , T.unpack cname
              , " but got: "
              , show xs
              ]

getColumn _ _ _ x  _ =
    return $ Left $ pack $ "Invalid result from INFORMATION_SCHEMA: " ++ show x

-- | Extra column information from MySQL schema
data ColumnInfo = ColumnInfo
  { ciColumnType :: Text
  , ciMaxLength :: Maybe Integer
  , ciNumericPrecision :: PersistValue
  , ciNumericScale :: PersistValue
  }

-- | Parse the type of column as returned by MySQL's
-- @INFORMATION_SCHEMA@ tables.
parseColumnType :: Text -> ColumnInfo -> ExceptT String IO (SqlType, Maybe Integer)
-- Ints
parseColumnType "tinyint" ci | ciColumnType ci == "tinyint(1)" = return (SqlBool, Nothing)
parseColumnType "int" ci | ciColumnType ci == "int(11)"        = return (SqlInt32, Nothing)
parseColumnType "bigint" ci | ciColumnType ci == "bigint(20)"  = return (SqlInt64, Nothing)
-- Double
parseColumnType x@("double") ci | ciColumnType ci == x         = return (SqlReal, Nothing)
parseColumnType "decimal" ci                                   =
  case (ciNumericPrecision ci, ciNumericScale ci) of
    (PersistInt64 p, PersistInt64 s) ->
      return (SqlNumeric (fromIntegral p) (fromIntegral s), Nothing)
    _ ->
      fail "missing DECIMAL precision in DB schema"
-- Text
parseColumnType "varchar" ci                                   = return (SqlString, ciMaxLength ci)
parseColumnType "text" _                                       = return (SqlString, Nothing)
-- ByteString
parseColumnType "varbinary" ci                                 = return (SqlBlob, ciMaxLength ci)
parseColumnType "blob" _                                       = return (SqlBlob, Nothing)
-- Time-related
parseColumnType "time" _                                       = return (SqlTime, Nothing)
parseColumnType "datetime" _                                   = return (SqlDayTime, Nothing)
parseColumnType "date" _                                       = return (SqlDay, Nothing)

parseColumnType _ ci                                           = return (SqlOther (ciColumnType ci), Nothing)


----------------------------------------------------------------------


-- | @getAlters allDefs tblName new old@ finds out what needs to
-- be changed from @old@ to become @new@.
getAlters
    :: [EntityDef]
    -> EntityDef
    -> ([Column], [(ConstraintNameDB, [FieldNameDB])])
    -> ([Column], [(ConstraintNameDB, [FieldNameDB])])
    -> ([AlterColumn], [AlterTable])
getAlters allDefs edef (c1, u1) (c2, u2) =
    (getAltersC c1 c2, getAltersU u1 u2)
  where
    tblName = getEntityDBName edef
    getAltersC [] old = concatMap dropColumn old
    getAltersC (new:news) old =
        let (alters, old') = findAlters edef allDefs new old
         in alters ++ getAltersC news old'

    dropColumn col =
        [DropReference (crConstraintName cr) | Just cr <- [cReference col]] ++
        [Drop col]

    getAltersU [] old = map (DropUniqueConstraint . fst) old
    getAltersU ((name, cols):news) old =
        case lookup name old of
            Nothing ->
                AddUniqueConstraint name (map findTypeAndMaxLen cols)
                : getAltersU news old
            Just ocols ->
                let old' = filter (\(x, _) -> x /= name) old
                 in if sort cols == ocols
                        then getAltersU news old'
                        else  DropUniqueConstraint name
                            : AddUniqueConstraint name (map findTypeAndMaxLen cols)
                            : getAltersU news old'
        where
          findTypeAndMaxLen col =
              let (col', ty) = findTypeOfColumn allDefs tblName col
                  (_, ml) = findMaxLenOfColumn allDefs tblName col
              in
                  (col', ty, ml)


-- | @findAlters x y newColumn oldColumns@ finds out what needs to be
-- changed in the columns @oldColumns@ for @newColumn@ to be
-- supported.
findAlters
    :: EntityDef
    -> [EntityDef]
    -> Column
    -> [Column]
    -> ([AlterColumn], [Column])
findAlters edef allDefs col@(Column name isNull type_ def gen _defConstraintName maxLen ref) cols =
    case filter ((name ==) . cName) cols of
    -- new fkey that didn't exist before
        [] ->
            case ref of
                Nothing -> ([Add' col],[])
                Just cr ->
                    let tname = crTableName cr
                        cname = crConstraintName cr
                        cnstr = [addReference allDefs cname tname name (crFieldCascade cr)]
                    in
                        (Add' col : cnstr, cols)
        Column _ isNull' type_' def' gen' _defConstraintName' maxLen' ref' : _ ->
            let -- Foreign key
                refDrop =
                    case (ref == ref', ref') of
                        (False, Just ColumnReference {crConstraintName=cname}) ->
                            [DropReference cname]
                        _ ->
                            []
                refAdd  =
                    case (ref == ref', ref) of
                        (False, Just ColumnReference {crTableName=tname, crConstraintName=cname, crFieldCascade = cfc })
                            | tname /= getEntityDBName edef
                            , Just idField <- getEntityIdField edef
                            , unConstraintNameDB cname /= unFieldNameDB (fieldDB idField)
                            ->
                            [addReference allDefs cname tname name cfc]
                        _ -> []
                -- Type and nullability
                modType | showSqlType type_ maxLen False `ciEquals` showSqlType type_' maxLen' False && isNull == isNull' = []
                        | otherwise = [Change col]

                -- Default value
                -- Avoid DEFAULT NULL, since it is always unnecessary, and is an error for text/blob fields
                modDef =
                    if def == def' then []
                    else case def of
                        Nothing -> [NoDefault col]
                        Just s ->
                            if T.toUpper s == "NULL" then []
                            else [Default col $ T.unpack s]

                -- Does the generated value need to change?
                modGen =
                    if gen == gen' then []
                    else case gen of
                        Nothing -> [NoGen col type_ maxLen]
                        Just genExpr -> [Gen col type_ maxLen $ T.unpack genExpr]

            in ( refDrop ++ modType ++ modDef ++ modGen ++ refAdd
               , filter ((name /=) . cName) cols
               )

  where
    ciEquals x y = T.toCaseFold (T.pack x) == T.toCaseFold (T.pack y)

----------------------------------------------------------------------


-- | Prints the part of a @CREATE TABLE@ statement about a given
-- column.
showColumn :: Column -> String
showColumn (Column n nu t def gen _defConstraintName maxLen ref) = concat
    [ escapeF n
    , " "
    , showSqlType t maxLen True
    , " "
    , case gen of
        Nothing -> ""
        Just genExpr ->
            if T.toUpper genExpr == "NULL" then ""
            else " GENERATED ALWAYS AS (" <> T.unpack genExpr <> ") STORED "
    , if nu then "NULL" else "NOT NULL"
    , case def of
        Nothing -> ""
        Just s -> -- Avoid DEFAULT NULL, since it is always unnecessary, and is an error for text/blob fields
                  if T.toUpper s == "NULL" then ""
                  else " DEFAULT " ++ T.unpack s
    , case ref of
        Nothing -> ""
        Just cRef -> " REFERENCES " ++ escapeE (crTableName cRef)
            <> " " <> T.unpack (renderFieldCascade (crFieldCascade cRef))
    ]


-- | Renders an 'SqlType' in MySQL's format.
showSqlType :: SqlType
            -> Maybe Integer -- ^ @maxlen@
            -> Bool -- ^ include character set information?
            -> String
showSqlType SqlBlob    Nothing    _     = "BLOB"
showSqlType SqlBlob    (Just i)   _     = "VARBINARY(" ++ show i ++ ")"
showSqlType SqlBool    _          _     = "TINYINT(1)"
showSqlType SqlDay     _          _     = "DATE"
showSqlType SqlDayTime _          _     = "DATETIME"
showSqlType SqlInt32   _          _     = "INT(11)"
showSqlType SqlInt64   _          _     = "BIGINT"
showSqlType SqlReal    _          _     = "DOUBLE"
showSqlType (SqlNumeric s prec) _ _     = "NUMERIC(" ++ show s ++ "," ++ show prec ++ ")"
showSqlType SqlString  Nothing    True  = "TEXT CHARACTER SET utf8mb4"
showSqlType SqlString  Nothing    False = "TEXT"
showSqlType SqlString  (Just i)   True  = "VARCHAR(" ++ show i ++ ") CHARACTER SET utf8mb4"
showSqlType SqlString  (Just i)   False = "VARCHAR(" ++ show i ++ ")"
showSqlType SqlTime    _          _     = "TIME"
showSqlType (SqlOther t) _        _     = T.unpack t

-- | Render an action that must be done on the database.
showAlterDb :: AlterDB -> (Bool, Text)
showAlterDb (AddTable s) = (False, pack s)
showAlterDb (AlterColumn t ac) =
    (isUnsafe ac, pack $ showAlter t ac)
  where
    isUnsafe Drop{} = True
    isUnsafe _      = False
showAlterDb (AlterTable t at) = (False, pack $ showAlterTable t at)


-- | Render an action that must be done on a table.
showAlterTable :: EntityNameDB -> AlterTable -> String
showAlterTable table (AddUniqueConstraint cname cols) = concat
    [ "ALTER TABLE "
    , escapeE table
    , " ADD CONSTRAINT "
    , escapeC cname
    , " UNIQUE("
    , intercalate "," $ map escapeDBName' cols
    , ")"
    ]
    where
      escapeDBName' (name, (FTTypeCon _ "Text"      ), maxlen) = escapeF name ++ "(" ++ show maxlen ++ ")"
      escapeDBName' (name, (FTTypeCon _ "String"    ), maxlen) = escapeF name ++ "(" ++ show maxlen ++ ")"
      escapeDBName' (name, (FTTypeCon _ "ByteString"), maxlen) = escapeF name ++ "(" ++ show maxlen ++ ")"
      escapeDBName' (name, _                         , _)      = escapeF name
showAlterTable table (DropUniqueConstraint cname) = concat
    [ "ALTER TABLE "
    , escapeE table
    , " DROP INDEX "
    , escapeC cname
    ]


-- | Render an action that must be done on a column.
showAlter :: EntityNameDB -> AlterColumn -> String
showAlter table (Change (Column n nu t def gen defConstraintName maxLen _ref)) =
    concat
    [ "ALTER TABLE "
    , escapeE table
    , " CHANGE "
    , escapeF n
    , " "
    , showColumn (Column n nu t def gen defConstraintName maxLen Nothing)
    ]
showAlter table (Add' col) =
    concat
    [ "ALTER TABLE "
    , escapeE table
    , " ADD COLUMN "
    , showColumn col
    ]
showAlter table (Drop c) =
    concat
    [ "ALTER TABLE "
    , escapeE table
    , " DROP COLUMN "
    , escapeF (cName c)
    ]
showAlter table (Default c s) =
    concat
    [ "ALTER TABLE "
    , escapeE table
    , " ALTER COLUMN "
    , escapeF (cName c)
    , " SET DEFAULT "
    , s
    ]
showAlter table (NoDefault c) =
    concat
    [ "ALTER TABLE "
    , escapeE table
    , " ALTER COLUMN "
    , escapeF (cName c)
    , " DROP DEFAULT"
    ]
showAlter table (Gen col typ len expr) =
    concat
    [ "ALTER TABLE "
    , escapeE table
    , " MODIFY COLUMN "
    , escapeF (cName col)
    , " "
    , showSqlType typ len True
    , " GENERATED ALWAYS AS ("
    , expr
    , ") STORED"
    ]
showAlter table (NoGen col typ len) =
    concat
    [ "ALTER TABLE "
    , escapeE table
    , " MODIFY COLUMN "
    , escapeF (cName col)
    , " "
    , showSqlType typ len True
    ]
showAlter table (Update' c s) =
    concat
    [ "UPDATE "
    , escapeE table
    , " SET "
    , escapeF (cName c)
    , "="
    , s
    , " WHERE "
    , escapeF (cName c)
    , " IS NULL"
    ]
showAlter table (AddReference reftable fkeyname t2 id2 fc) = concat
    [ "ALTER TABLE "
    , escapeE table
    , " ADD CONSTRAINT "
    , escapeC fkeyname
    , " FOREIGN KEY("
    , intercalate "," $ map escapeF t2
    , ") REFERENCES "
    , escapeE reftable
    , "("
    , intercalate "," $ map escapeF id2
    , ") "
    , T.unpack $ renderFieldCascade fc
    ]
showAlter table (DropReference cname) = concat
    [ "ALTER TABLE "
    , escapeE table
    , " DROP FOREIGN KEY "
    , escapeC cname
    ]

----------------------------------------------------------------------

escapeC :: ConstraintNameDB -> String
escapeC = escapeWith (escapeDBName . T.unpack)

escapeE :: EntityNameDB -> String
escapeE = escapeWith (escapeDBName . T.unpack)

escapeF :: FieldNameDB -> String
escapeF = escapeWith (escapeDBName . T.unpack)

escapeET :: EntityNameDB -> Text
escapeET = escapeWith (T.pack . escapeDBName . T.unpack)

escapeFT :: FieldNameDB -> Text
escapeFT = escapeWith (T.pack . escapeDBName . T.unpack)

-- | Escape a database name to be included on a query.
escapeDBName :: String -> String
escapeDBName str = '`' : go str
    where
      go ('`':xs) = '`' : '`' : go xs
      go ( x :xs) =     x     : go xs
      go ""       = "`"

-- | Information required to connect to a MySQL database
-- using @persistent@'s generic facilities.  These values are the
-- same that are given to 'withMySQLPool'.
data MySQLConf = MySQLConf
    MySQLConnectInfo
    Int
    deriving Show

-- | Extract connection configs from 'MySQLConf'
-- @since 0.4.1
myConnInfo :: MySQLConf -> MySQLConnectInfo
myConnInfo (MySQLConf c _) = c

-- | Extract connection pool size from 'MySQLConf'
-- @since 0.4.1
myPoolSize :: MySQLConf -> Int
myPoolSize (MySQLConf _ p) = p

setMyConnInfo :: MySQLConnectInfo -> MySQLConf -> MySQLConf
setMyConnInfo c (MySQLConf _ p) = MySQLConf c p

-- | Public constructor for @MySQLConf@.
mkMySQLConf
  :: MySQLConnectInfo  -- ^ The connection information.
  -> Int               -- ^ How many connections should be held on the connection pool.
  -> MySQLConf
mkMySQLConf = MySQLConf

-- | MySQL connection information.
data MySQLConnectInfo = MySQLConnectInfo
  { innerConnInfo :: MySQL.ConnectInfo
  , innerConnTLS  :: (Maybe TLS.ClientParams)
  } deriving Show

-- | Public constructor for @MySQLConnectInfo@.
mkMySQLConnectInfo :: MySQL.ConnectInfo -> MySQLConnectInfo
mkMySQLConnectInfo innerCi = MySQLConnectInfo innerCi Nothing

-- | Update port number for @MySQLConnectInfo@.
setMySQLConnectInfoPort
  :: NetworkSocket.PortNumber -> MySQLConnectInfo -> MySQLConnectInfo
setMySQLConnectInfoPort port ci
  = ci {innerConnInfo = innerCi { MySQL.ciPort = port } }
  where innerCi = innerConnInfo ci

-- | Update character set for @MySQLConnectInfo@.
setMySQLConnectInfoCharset
  :: Word.Word8       -- ^ Numeric ID of collation. See https://dev.mysql.com/doc/refman/5.7/en/show-collation.html.
  -> MySQLConnectInfo -- ^ Reference connectInfo to perform update on
  -> MySQLConnectInfo
setMySQLConnectInfoCharset charset ci
  = ci {innerConnInfo = innerCi { MySQL.ciCharset = charset } }
  where innerCi = innerConnInfo ci

-- | Set TLS ClientParams for @MySQLConnectInfo@.
setMySQLConnectInfoTLS
  :: TLS.ClientParams -- ^ @ClientParams@ to establish a TLS connection with.
  -> MySQLConnectInfo -- ^ Reference connectInfo to perform update on
  -> MySQLConnectInfo
setMySQLConnectInfoTLS tls ci
  = ci {innerConnTLS = Just tls}

instance FromJSON MySQLConf where
    parseJSON v = modifyFailure ("Persistent: error loading MySQL conf: " ++) $
      flip (withObject "MySQLConf") v $ \o -> do
        database <- o .: "database"
        host     <- o .: "host"
        port     <- o .: "port"
        user     <- o .: "user"
        password <- o .: "password"
        pool     <- o .: "poolsize"
        let ci = MySQL.defaultConnectInfo
                   { MySQL.ciHost     = host
                   , MySQL.ciPort     = fromIntegral (port :: Word)
                   , MySQL.ciUser     = BSC.pack user
                   , MySQL.ciPassword = BSC.pack password
                   , MySQL.ciDatabase = BSC.pack database
                   }
        return $ MySQLConf (MySQLConnectInfo ci Nothing) pool

instance PersistConfig MySQLConf where
    type PersistConfigBackend MySQLConf = SqlPersistT

    type PersistConfigPool    MySQLConf = ConnectionPool

    createPoolConfig (MySQLConf cs size)
      = runNoLoggingT $ createMySQLPool cs size -- FIXME

    runPool _ = runSqlPool

    loadConfig = parseJSON

    applyEnv conf = do
        env <- getEnvironment
        let maybeEnv old var = maybe old id $ fmap BSC.pack $ lookup ("MYSQL_" ++ var) env
        let innerCi = innerConnInfo . myConnInfo $ conf
        let innerCiNew = case innerCi of
                MySQL.ConnectInfo
                  { MySQL.ciHost     = host
                  , MySQL.ciPort     = port
                  , MySQL.ciUser     = user
                  , MySQL.ciPassword = password
                  , MySQL.ciDatabase = database
                  } -> (innerCi)
                        { MySQL.ciHost     = BSC.unpack $ maybeEnv (BSC.pack host) "HOST"
                        , MySQL.ciPort     = read (BSC.unpack $ maybeEnv (BSC.pack $ show port) "PORT")
                        , MySQL.ciUser     = maybeEnv user "USER"
                        , MySQL.ciPassword = maybeEnv password "PASSWORD"
                        , MySQL.ciDatabase = maybeEnv database "DATABASE"
                        }
        return $ setMyConnInfo (MySQLConnectInfo innerCiNew Nothing) conf

mockMigrate :: MySQL.ConnectInfo
         -> [EntityDef]
         -> (Text -> IO Statement)
         -> EntityDef
         -> IO (Either [Text] [(Bool, Text)])
mockMigrate _connectInfo allDefs _getter val = do
    let name = getEntityDBName val
    let (newcols, udefs, fdefs) = mysqlMkColumns allDefs val
    let udspair = map udToPair udefs
    case () of
      -- Nothing found, create everything
      () -> do
        let uniques = flip concatMap udspair $ \(uname, ucols) ->
                      [ AlterTable name $
                        AddUniqueConstraint uname $
                        map (findTypeAndMaxLen name) ucols ]
        let foreigns = do
              Column { cName=cname, cReference= Just ColumnReference{crTableName = refTable, crConstraintName = refConstr, crFieldCascade = cfc }} <- newcols
              return $ AlterColumn name (addReference allDefs refConstr refTable cname cfc)

        let foreignsAlt =
                map
                    (\fdef ->
                        let (childfields, parentfields) = unzip (map (\((_,b),(_,d)) -> (b,d)) (foreignFields fdef))
                        in
                            AlterColumn
                                name
                                (AddReference
                                    (foreignRefTableDBName fdef)
                                    (foreignConstraintNameDBName fdef)
                                    childfields
                                    parentfields
                                    (foreignFieldCascade fdef)
                                )
                    )
                    fdefs

        return $ Right $ map showAlterDb $ (addTable newcols val): uniques ++ foreigns ++ foreignsAlt

      where
        findTypeAndMaxLen tblName col = let (col', ty) = findTypeOfColumn allDefs tblName col
                                            (_, ml) = findMaxLenOfColumn allDefs tblName col
                                         in (col', ty, ml)


-- | Mock a migration even when the database is not present.
-- This function will mock the migration for a database even when
-- the actual database isn't already present in the system.
mockMigration :: Migration -> IO ()
mockMigration mig = do
    smap <- newIORef $ Map.empty
    let sqlbackend =
            mkSqlBackend SqlBackend.MkSqlBackendArgs
                { SqlBackend.connPrepare = \_ -> do
                    return Statement
                        { stmtFinalize = return ()
                        , stmtReset = return ()
                        , stmtExecute = undefined
                        , stmtQuery = \_ -> return $ return ()
                        }
                , SqlBackend.connInsertSql = undefined
                , SqlBackend.connStmtMap = smap
                , SqlBackend.connClose = undefined
                , SqlBackend.connMigrateSql = mockMigrate undefined
                , SqlBackend.connBegin = undefined
                , SqlBackend.connCommit = undefined
                , SqlBackend.connRollback = undefined
                , SqlBackend.connEscapeFieldName = T.pack . escapeDBName . T.unpack . unFieldNameDB
                , SqlBackend.connEscapeTableName = T.pack . escapeDBName . T.unpack . unEntityNameDB . getEntityDBName
                , SqlBackend.connEscapeRawName = T.pack . escapeDBName . T.unpack
                , SqlBackend.connNoLimit = undefined
                , SqlBackend.connRDBMS = undefined
                , SqlBackend.connLimitOffset = undefined
                , SqlBackend.connLogFunc = undefined
                }
        result = runReaderT . runWriterT . runWriterT $ mig
    resp <- result sqlbackend
    mapM_ T.putStrLn $ map snd $ snd resp

-- | MySQL specific 'upsert_'. This will prevent multiple queries, when one will
-- do. The record will be inserted into the database. In the event that the
-- record already exists in the database, the record will have the
-- relevant updates performed.
insertOnDuplicateKeyUpdate
  :: ( backend ~ PersistEntityBackend record
     , PersistEntity record
     , MonadIO m
     , PersistStore backend
     , BackendCompatible SqlBackend backend
     )
  => record
  -> [Update record]
  -> ReaderT backend m ()
insertOnDuplicateKeyUpdate record =
  insertManyOnDuplicateKeyUpdate [record] []

-- | Combination of 'insertOnDuplicateKeyUpdate' and 'insertKey'.
--   @since 5.1.0
insertEntityOnDuplicateKeyUpdate
  :: ( backend ~ PersistEntityBackend record
     , PersistEntity record
     , MonadIO m
     , PersistStore backend
     , BackendCompatible SqlBackend backend
     )
  => Entity record
  -> [Update record]
  -> ReaderT backend m ()
insertEntityOnDuplicateKeyUpdate entity =
  insertEntityManyOnDuplicateKeyUpdate [entity] []


-- | This type is used to determine how to update rows using MySQL's
-- @INSERT ... ON DUPLICATE KEY UPDATE@ functionality, exposed via
-- 'insertManyOnDuplicateKeyUpdate' in this library.
--
-- @since 2.8.0
data HandleUpdateCollision record where
    -- | Copy the field directly from the record.
    CopyField :: EntityField record typ -> HandleUpdateCollision record
    -- | Only copy the field if it is not equal to the provided value.
    CopyUnlessEq :: PersistField typ => EntityField record typ -> typ -> HandleUpdateCollision record
    -- | Only copy the field if the new value is greater than the existing value.
    CopyWhenGreater :: PersistField typ => EntityField record typ -> HandleUpdateCollision record

-- | Copy the field into the database only if its new value is greater than existing one.
copyWhenGreater :: PersistField typ => EntityField record typ -> HandleUpdateCollision record
copyWhenGreater = CopyWhenGreater

-- | Copy the field into the database only if the value in the
-- corresponding record is non-@NULL@.
--
-- @since  2.6.2
copyUnlessNull :: PersistField typ => EntityField record (Maybe typ) -> HandleUpdateCollision record
copyUnlessNull field = CopyUnlessEq field Nothing

-- | Copy the field into the database only if the value in the
-- corresponding record is non-empty, where "empty" means the Monoid
-- definition for 'mempty'. Useful for 'Text', 'String', 'ByteString', etc.
--
-- The resulting 'HandleUpdateCollision' type is useful for the
-- 'insertManyOnDuplicateKeyUpdate' function.
--
-- @since  2.6.2
copyUnlessEmpty :: (Monoid.Monoid typ, PersistField typ) => EntityField record typ -> HandleUpdateCollision record
copyUnlessEmpty field = CopyUnlessEq field Monoid.mempty

-- | Copy the field into the database only if the field is not equal to the
-- provided value. This is useful to avoid copying weird nullary data into
-- the database.
--
-- The resulting 'HandleUpdateCollision' type is useful for the
-- 'insertManyOnDuplicateKeyUpdate' function.
--
-- @since  2.6.2
copyUnlessEq :: PersistField typ => EntityField record typ -> typ -> HandleUpdateCollision record
copyUnlessEq = CopyUnlessEq

-- | Copy the field directly from the record.
--
-- @since 3.0
copyField :: PersistField typ => EntityField record typ -> HandleUpdateCollision record
copyField = CopyField

-- | Do a bulk insert on the given records in the first parameter. In the event
-- that a key conflicts with a record currently in the database, the second and
-- third parameters determine what will happen.
--
-- The second parameter is a list of fields to copy from the original value.
-- This allows you to specify which fields to copy from the record you're trying
-- to insert into the database to the preexisting row.
--
-- The third parameter is a list of updates to perform that are independent of
-- the value that is provided. You can use this to increment a counter value.
-- These updates only occur if the original record is present in the database.
--
-- === __More details on 'HandleUpdateCollision' usage__
--
-- The @['HandleUpdateCollision']@ parameter allows you to specify which fields (and
-- under which conditions) will be copied from the inserted rows. For
-- a brief example, consider the following data model and existing data set:
--
-- @
-- Item
--   name        Text
--   description Text
--   price       Double Maybe
--   quantity    Int Maybe
--
--   Primary name
-- @
--
-- > items:
-- > +------+-------------+-------+----------+
-- > | name | description | price | quantity |
-- > +------+-------------+-------+----------+
-- > | foo  | very good   |       |    3     |
-- > | bar  |             |  3.99 |          |
-- > +------+-------------+-------+----------+
--
-- This record type has a single natural key on @itemName@. Let's suppose
-- that we download a CSV of new items to store into the database. Here's
-- our CSV:
--
-- > name,description,price,quantity
-- > foo,,2.50,6
-- > bar,even better,,5
-- > yes,wow,,
--
-- We parse that into a list of Haskell records:
--
-- @
-- records =
--   [ Item { itemName = "foo", itemDescription = ""
--          , itemPrice = Just 2.50, itemQuantity = Just 6
--          }
--   , Item "bar" "even better" Nothing (Just 5)
--   , Item "yes" "wow" Nothing Nothing
--   ]
-- @
--
-- The new CSV data is partial. It only includes __updates__ from the
-- upstream vendor. Our CSV library parses the missing description field as
-- an empty string. We don't want to override the existing description. So
-- we can use the 'copyUnlessEmpty' function to say: "Don't update when the
-- value is empty."
--
-- Likewise, the new row for @bar@ includes a quantity, but no price. We do
-- not want to overwrite the existing price in the database with a @NULL@
-- value. So we can use 'copyUnlessNull' to only copy the existing values
-- in.
--
-- The final code looks like this:
-- @
-- 'insertManyOnDuplicateKeyUpdate' records
--   [ 'copyUnlessEmpty' ItemDescription
--   , 'copyUnlessNull' ItemPrice
--   , 'copyUnlessNull' ItemQuantity
--   ]
--   []
-- @
--
-- Once we run that code on the database, the new data set looks like this:
--
-- > items:
-- > +------+-------------+-------+----------+
-- > | name | description | price | quantity |
-- > +------+-------------+-------+----------+
-- > | foo  | very good   |  2.50 |    6     |
-- > | bar  | even better |  3.99 |    5     |
-- > | yes  | wow         |       |          |
-- > +------+-------------+-------+----------+
insertManyOnDuplicateKeyUpdate
    :: forall record backend m.
    ( backend ~ PersistEntityBackend record
    , BackendCompatible SqlBackend backend
    , PersistEntity record
    , MonadIO m
    )
    => [record] -- ^ A list of the records you want to insert, or update
    -> [HandleUpdateCollision record] -- ^ A list of the fields you want to copy over.
    -> [Update record] -- ^ A list of the updates to apply that aren't dependent on the record being inserted.
    -> ReaderT backend m ()
insertManyOnDuplicateKeyUpdate [] _ _ = return ()
insertManyOnDuplicateKeyUpdate records fieldValues updates =
    uncurry rawExecute
    $ mkBulkInsertQuery (Left records) fieldValues updates

-- | Combination of 'insertManyOnDuplicateKeyUpdate' and 'insertEntityMany'
--   @since 5.1.0
insertEntityManyOnDuplicateKeyUpdate
    :: forall record backend m.
    ( backend ~ PersistEntityBackend record
    , BackendCompatible SqlBackend backend
    , PersistEntity record
    , MonadIO m
    )
    => [Entity record] -- ^ A list of the records you want to insert, or update
    -> [HandleUpdateCollision record] -- ^ A list of the fields you want to copy over.
    -> [Update record] -- ^ A list of the updates to apply that aren't dependent on the record being inserted.
    -> ReaderT backend m ()
insertEntityManyOnDuplicateKeyUpdate [] _ _ = return ()
insertEntityManyOnDuplicateKeyUpdate entities fieldValues updates =
    uncurry rawExecute
    $ mkBulkInsertQuery (Right entities) fieldValues updates


-- | This creates the query for 'bulkInsertOnDuplicateKeyUpdate'. If you
-- provide an empty list of updates to perform, then it will generate
-- a dummy/no-op update using the first field of the record. This avoids
-- duplicate key exceptions.
mkBulkInsertQuery
    :: PersistEntity record
    => Either [record] [Entity record] -- ^ A list of the records you want to insert, or update, possibly with keys
    -> [HandleUpdateCollision record] -- ^ A list of the fields you want to copy over.
    -> [Update record] -- ^ A list of the updates to apply that aren't dependent on the record being inserted.
    -> (Text, [PersistValue])
mkBulkInsertQuery records fieldValues updates =
    (q, recordValues <> updsValues <> copyUnlessValues)
  where
    collectFieldDef !x (compareCopyList, toMaybeCopyList, fieldNamesList) = case x of
      CopyField rec ->
        let !fieldText = fieldDbToText (persistFieldDef rec)
        in (compareCopyList, toMaybeCopyList, fieldText : fieldNamesList)
      CopyUnlessEq rec val ->
        let !fieldText = (fieldDbToText (persistFieldDef rec), toPersistValue val)
        in (compareCopyList, fieldText : toMaybeCopyList, fieldNamesList)
      CopyWhenGreater rec ->
        let !fieldText = fieldDbToText (persistFieldDef rec)
        in (fieldText : compareCopyList, toMaybeCopyList, fieldNamesList)
    (fieldsCompareCopy, fieldsToMaybeCopy, updateFieldNames)
      = foldr collectFieldDef ([], [], []) fieldValues
    fieldDbToText = T.pack . escapeF . fieldDB
    entityDef' = entityDef $ either id (map entityVal) records
    firstField = case entityFieldNames of
        [] -> error "The entity you're trying to insert does not have any fields."
        (field:_) -> field
    entityFieldNames = map fieldDbToText $ case records of
      Left _  -> getEntityFieldsDatabase entityDef'
      Right _ -> NEL.toList (keyAndEntityFields entityDef')
    tableName = T.pack . escapeE . getEntityDBName $ entityDef'
    copyUnlessValues = map snd fieldsToMaybeCopy
    values = either (map $ map toPersistValue . toPersistFields) (map entityValues) records
    recordValues = concat values
    recordPlaceholders = Util.commaSeparated $ map (Util.parenWrapped . Util.commaSeparated . map (const "?")) values
    mkCondFieldSet n _ = T.concat
        [ n
        , "=COALESCE("
        ,   "NULLIF("
        ,     "VALUES(", n, "),"
        ,     "?"
        ,   "),"
        ,   n
        , ")"
        ]
    mkCompareFieldSet n = T.concat
      [ n
      , "=IF("
      ,   n, " < VALUES(", n, "),"
      ,   "VALUES(", n, "),", n
      , ")"
      ]
    condFieldSets = map (uncurry mkCondFieldSet) fieldsToMaybeCopy
    compareFieldSets = map mkCompareFieldSet fieldsCompareCopy
    fieldSets = map (\n -> T.concat [n, "=VALUES(", n, ")"]) updateFieldNames
    upds = map (Util.mkUpdateText' (pack . escapeF) id) updates
    updsValues = map (\(Update _ val _) -> toPersistValue val) updates
    updateText = case fieldSets <> upds <> condFieldSets <> compareFieldSets of
        [] -> T.concat [firstField, "=", firstField]
        xs -> Util.commaSeparated xs
    q = T.concat
        [ "INSERT INTO "
        , tableName
        , " ("
        , Util.commaSeparated entityFieldNames
        , ") "
        , " VALUES "
        , recordPlaceholders
        , " ON DUPLICATE KEY UPDATE "
        , updateText
        ]

putManySql :: EntityDef -> Int -> Text
putManySql ent n = putManySql' fields ent n
  where
    fields = getEntityFields ent

repsertManySql :: EntityDef -> Int -> Text
repsertManySql ent n = putManySql' fields ent n
  where
    fields = NEL.toList $ keyAndEntityFields ent

putManySql' :: [FieldDef] -> EntityDef -> Int -> Text
putManySql' (filter isFieldNotGenerated -> fields) ent n = q
  where
    fieldDbToText = (T.pack . escapeF) . fieldDB
    mkAssignment f = T.concat [f, "=VALUES(", f, ")"]

    table = (T.pack . escapeE) . getEntityDBName $ ent
    columns = Util.commaSeparated $ map fieldDbToText fields
    placeholders = map (const "?") fields
    updates = map (mkAssignment . fieldDbToText) fields

    q = T.concat
        [ "INSERT INTO "
        , table
        , Util.parenWrapped columns
        , " VALUES "
        , Util.commaSeparated . replicate n
            . Util.parenWrapped . Util.commaSeparated $ placeholders
        , " ON DUPLICATE KEY UPDATE "
        , Util.commaSeparated updates
        ]

mysqlMkColumns :: [EntityDef] -> EntityDef -> ([Column], [UniqueDef], [ForeignDef])
mysqlMkColumns allDefs t = mkColumns allDefs t emptyBackendSpecificOverrides
