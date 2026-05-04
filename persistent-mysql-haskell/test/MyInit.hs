{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module MyInit (
  (@/=), (@==), (==@)
  , asIO
  , assertNotEqual
  , assertNotEmpty
  , assertEmpty
  , isTravis
  , BackendMonad
  , runConn

  , MonadIO
  , persistSettings
  , MkPersistSettings (..)
  , db
  , BackendKey(..)
  , GenerateKey(..)

  , RunDb
   -- re-exports
  , module Database.Persist
  , module Database.Persist.Sql.Raw.QQ
  , module Test.Hspec
  , module Test.HUnit
  , MonadUnliftIO
  , liftIO
  , mkPersist, mkMigrate, share, sqlSettings, persistLowerCase, persistUpperCase
  , mkEntityDefList, sqlSettingsUuid
  , Int32, Int64
  , Text
  , module Control.Monad.Trans.Reader
  , module Control.Monad
  , module Database.Persist.Sql
  , BS.ByteString
  , migrateModels
  , SomeException
  , MonadFail
  , TestFn(..)
  , truncateTimeOfDay
  , truncateToMicro
  , truncateUTCTime
  , arbText
  , liftA2
  , LoggingT, ResourceT, UUID(..)
  ) where

import Init
       ( GenerateKey(..)
       , MonadFail
       , RunDb
       , TestFn(..)
       , arbText
       , asIO
       , assertEmpty
       , assertNotEmpty
       , assertNotEqual
       , isTravis
       , truncateTimeOfDay
       , truncateToMicro
       , truncateUTCTime
       , (==@)
       , (@/=)
       , (@==)
       )

-- re-exports
import Control.Applicative (liftA2)
import Control.Exception (SomeException)
import Control.Monad (forM_, liftM, replicateM, void, when)
import Control.Monad.Trans.Reader
import Data.Aeson (FromJSON, ToJSON, Value(..))
import Database.Persist.Sql.Raw.QQ
import Database.Persist.TH
       ( MkPersistSettings(..)
       , migrateModels
       , setImplicitIdDef
       , mkEntityDefList
       , mkMigrate
       , mkPersist
       , persistLowerCase
       , persistUpperCase
       , share
       , sqlSettings
       )
import Test.Hspec
import Test.QuickCheck.Instances ()
import Web.Internal.HttpApiData
import Web.PathPieces
import Database.Persist.ImplicitIdDef

-- testing
import Test.HUnit (Assertion, assertBool, assertFailure, (@=?), (@?=))

import Control.Monad (unless, (>=>))
import Control.Monad.IO.Class
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Control.Monad.Logger
import Control.Monad.Trans.Resource (ResourceT, runResourceT)
import qualified Data.ByteString as BS
import Data.Int (Int32, Int64)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import qualified Database.MySQL.Base as MySQL
import System.Log.FastLogger (fromLogStr)

import Database.Persist
import Database.Persist.MySQL
import Database.Persist.Sql

_debugOn :: Bool
_debugOn = False

persistSettings :: MkPersistSettings
persistSettings = sqlSettings { mpsGeneric = True }

type BackendMonad = SqlBackend

runConn :: MonadUnliftIO m => SqlPersistT (LoggingT m) t -> m ()
runConn f = do
  travis <- liftIO isTravis
  let debugPrint = not travis && _debugOn
  let printDebug = if debugPrint then print . fromLogStr else void . return
  let ff = rawExecute "SET SESSION sql_mode = ''" [] >> f
  flip runLoggingT (\_ _ _ s -> printDebug s) $ do
    void
        $ withMySQLPool
            ( mkMySQLConnectInfo
                ( MySQL.defaultConnectInfo
                    { MySQL.ciHost     = "localhost"
                    , MySQL.ciUser     = "test"
                    , MySQL.ciPassword = "test"
                    , MySQL.ciDatabase = "test"
                    , MySQL.ciPort =
                        if travis
                            then 33306
                            else MySQL.ciPort MySQL.defaultConnectInfo
                    }))
            1
        $ runSqlPool ff

db :: SqlPersistT (LoggingT (ResourceT IO)) () -> Assertion
db actions = do
  runResourceT $ runConn $ actions >> transactionUndo

newtype UUID = UUID { unUUID :: Text }
    deriving stock
        (Show, Eq, Ord, Read)
    deriving newtype
        ( ToJSON, FromJSON
        , PersistField, PersistFieldSql
        , FromHttpApiData, ToHttpApiData, PathPiece
        )

sqlSettingsUuid :: Text -> MkPersistSettings
sqlSettingsUuid defExpr =
    let
        uuidDef =
           setImplicitIdDefMaxLen 100 $ mkImplicitIdDef @UUID defExpr
        settings =
            setImplicitIdDef uuidDef sqlSettings
     in
        settings
