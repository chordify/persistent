{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts, FlexibleInstances, MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans -fno-warn-missing-fields #-}
-- overlapping instances is for automatic lifting
-- while avoiding an orphan of Lift for Text
{-# LANGUAGE OverlappingInstances #-}
-- | This module provides utilities for creating backends. Regular users do not
-- need to use this module.
module Database.Persist.TH
    ( -- * Parse entity defs
      persistWith
    , persistUpperCase
    , persistLowerCase
    , persistFileWith
      -- * Turn @EntityDef@s into types
    , mkPersist
    , MkPersistSettings
    , mpsBackend
    , mpsGeneric
    , mpsPrefixFields
    , mpsEntityJSON
    , mpsGenerateLenses
    , EntityJSON(..)
    , mkPersistSettings
    , sqlSettings
    , sqlOnlySettings
      -- * Various other TH functions
    , mkMigrate
    , mkSave
    , mkDeleteCascade
    , share
    , derivePersistField
    , derivePersistFieldJSON
    , persistFieldFromEntity
      -- * Internal
    , packPTH
    , lensPTH
    ) where

import Prelude hiding ((++), take, concat, splitAt, exp)
import Database.Persist
import Database.Persist.Sql (Migration, migrate, SqlBackend, PersistFieldSql)
import Database.Persist.Quasi
import Language.Haskell.TH.Lib (varE)
import Language.Haskell.TH.Quote
import Language.Haskell.TH.Syntax
import Data.Char (toLower, toUpper)
import Control.Monad (forM, (<=<), mzero)
import qualified System.IO as SIO
import Data.Text (pack, Text, append, unpack, concat, uncons, cons, stripPrefix, stripSuffix)
import Data.Text.Encoding (decodeUtf8)
import qualified Data.Text.IO as TIO
import Data.List (foldl')
import Data.Maybe (isJust, listToMaybe, mapMaybe, fromMaybe)
import Data.Monoid (mappend, mconcat)
import Text.Read (readPrec, lexP, step, prec, parens, Lexeme(Ident))
import qualified Data.Map as M
import qualified Data.HashMap.Strict as HM
import Data.Aeson
    ( ToJSON (toJSON), FromJSON (parseJSON), (.=), object
    , Value (Object), (.:), (.:?)
    , eitherDecodeStrict'
    )
import Control.Applicative (pure, (<$>), (<*>))
import Database.Persist.Sql (sqlType)
import Data.Proxy (Proxy (Proxy))
import Web.PathPieces (PathPiece, toPathPiece, fromPathPiece)
import GHC.Generics (Generic)
import qualified Data.Text.Encoding as TE

-- | Converts a quasi-quoted syntax into a list of entity definitions, to be
-- used as input to the template haskell generation code (mkPersist).
persistWith :: PersistSettings -> QuasiQuoter
persistWith ps = QuasiQuoter
    { quoteExp = parseReferences ps . pack
    }

-- | Apply 'persistWith' to 'upperCaseSettings'.
persistUpperCase :: QuasiQuoter
persistUpperCase = persistWith upperCaseSettings

-- | Apply 'persistWith' to 'lowerCaseSettings'.
persistLowerCase :: QuasiQuoter
persistLowerCase = persistWith lowerCaseSettings

-- | Same as 'persistWith', but uses an external file instead of a
-- quasiquotation.
persistFileWith :: PersistSettings -> FilePath -> Q Exp
persistFileWith ps fp = do
#ifdef GHC_7_4
    qAddDependentFile fp
#endif
    h <- qRunIO $ SIO.openFile fp SIO.ReadMode
    qRunIO $ SIO.hSetEncoding h SIO.utf8_bom
    s <- qRunIO $ TIO.hGetContents h
    parseReferences ps s

-- calls parse to Quasi.parse individual entities in isolation
-- afterwards, sets references to other entities
parseReferences :: PersistSettings -> Text -> Q Exp
parseReferences ps s = lift $
     map (mkEntityDefSqlTypeExp entityMap) entsWithEmbeds
  where
    -- every EntityDef could reference each-other (as an EmbedRef)
    -- let Haskell tie the knot
    entityMap = M.fromList $ map (\ent -> (entityHaskell ent, toEmbedEntityDef ent)) entsWithEmbeds
    entsWithEmbeds = map setEmbedEntity rawEnts
    setEmbedEntity ent = ent
      { entityFields = map (setEmbedField (entityHaskell ent) entityMap) $ entityFields ent
      }
    rawEnts = parse ps s


stripId :: FieldType -> Maybe Text
stripId (FTTypeCon Nothing t) = stripSuffix "Id" t
stripId _ = Nothing

foreignReference :: FieldDef -> Maybe HaskellName
foreignReference field = case fieldReference field of
    ForeignRef ref _ -> Just ref
    _              -> Nothing


-- fieldSqlType at parse time can be an Exp
-- This helps delay setting fieldSqlType until lift time
data EntityDefSqlTypeExp = EntityDefSqlTypeExp EntityDef SqlTypeExp [SqlTypeExp]
                           deriving Show

data SqlTypeExp = SqlTypeExp FieldType
                | SqlType' SqlType
                deriving Show

instance Lift SqlTypeExp where
    lift (SqlType' t)       = lift t
    lift (SqlTypeExp ftype) = return st
      where
        typ = ftToType ftype
        mtyp = (ConT ''Proxy `AppT` typ)
        typedNothing = SigE (ConE 'Proxy) mtyp
        st = VarE 'sqlType `AppE` typedNothing

data FieldsSqlTypeExp = FieldsSqlTypeExp [FieldDef] [SqlTypeExp]

instance Lift FieldsSqlTypeExp where
    lift (FieldsSqlTypeExp fields sqlTypeExps) =
        lift $ zipWith FieldSqlTypeExp fields sqlTypeExps

data FieldSqlTypeExp = FieldSqlTypeExp FieldDef SqlTypeExp
instance Lift FieldSqlTypeExp where
    lift (FieldSqlTypeExp (FieldDef{..}) sqlTypeExp) =
      [|FieldDef fieldHaskell fieldDB fieldType $(lift sqlTypeExp) fieldAttrs fieldStrict fieldReference|]

instance Lift EntityDefSqlTypeExp where
    lift (EntityDefSqlTypeExp ent sqlTypeExp sqlTypeExps) =
        [|ent { entityFields = $(lift $ FieldsSqlTypeExp (entityFields ent) sqlTypeExps)
              , entityId = $(lift $ FieldSqlTypeExp (entityId ent) sqlTypeExp)
              }
        |]

instance Lift ReferenceDef where
    lift NoReference = [|NoReference|]
    lift (ForeignRef name ft) = [|ForeignRef name ft|]
    lift (EmbedRef em) = [|EmbedRef em|]
    lift (CompositeRef cdef) = [|CompositeRef cdef|]
    lift SelfReference = [|SelfReference|]

instance Lift EmbedEntityDef where
    lift (EmbedEntityDef name fields) = [|EmbedEntityDef name fields|]

instance Lift EmbedFieldDef where
    lift (EmbedFieldDef name em) = [|EmbedFieldDef name em|]

type EntityMap = M.Map HaskellName EmbedEntityDef
mEmbedded :: EntityMap -> FieldType -> Maybe EmbedEntityDef
mEmbedded _ (FTTypeCon Just{} _) = Nothing
mEmbedded ents (FTTypeCon Nothing n) = let name = HaskellName n in
    M.lookup name ents
mEmbedded ents (FTList x) = mEmbedded ents x
mEmbedded ents (FTApp x y) = maybe (mEmbedded ents y) Just (mEmbedded ents x)

setEmbedField :: HaskellName -> EntityMap -> FieldDef -> FieldDef
setEmbedField entName allEntities field = field
  { fieldReference = case fieldReference field of
      NoReference ->
        case mEmbedded allEntities (fieldType field) of
            Nothing -> case stripId $ fieldType field of
                Nothing -> NoReference
                Just name -> if M.member (HaskellName name) allEntities
                    then ForeignRef (HaskellName name)
                                    -- the EmebedEntityDef does not contain FieldType information
                                    -- but we shouldn't need this anyway
                                    (FTTypeCon Nothing $ pack $ nameBase ''Int)
                    else NoReference
            Just em -> if embeddedHaskell em == entName then SelfReference else EmbedRef em
      existing@_   -> existing
  }

mkEntityDefSqlTypeExp :: EntityMap -> EntityDef -> EntityDefSqlTypeExp
mkEntityDefSqlTypeExp allEntities ent = EntityDefSqlTypeExp ent
    (getSqlType $ entityId ent)
    $ (map getSqlType $ entityFields ent)
  where
    getSqlType field = maybe
        (defaultSqlTypeExp field)
        (SqlType' . SqlOther)
        (listToMaybe $ mapMaybe (stripPrefix "sqltype=") $ fieldAttrs field)


    -- In the case of embedding, there won't be any datatype created yet.
    -- We just use SqlString, as the data will be serialized to JSON.
    defaultSqlTypeExp field
        | isJust (mEmbedded allEntities ftype) = SqlType' SqlString
        | otherwise = case fieldReference field of
            ForeignRef _ ft  -> SqlTypeExp ft
            CompositeRef _  -> SqlType' $ SqlOther "Composite Reference"
            _ -> case ftype of
                    -- In the case of lists, we always serialize to a string
                    -- value (via JSON).
                    --
                    -- Normally, this would be determined automatically by
                    -- SqlTypeExp. However, there's one corner case: if there's
                    -- a list of entity IDs, the datatype for the ID has not
                    -- yet been created, so the compiler will fail. This extra
                    -- clause works around this limitation.
                    FTList _ -> SqlType' SqlString
                    _ -> SqlTypeExp ftype
      where
        ftype = fieldType field

-- | Create data types and appropriate 'PersistEntity' instances for the given
-- 'EntityDef's. Works well with the persist quasi-quoter.
mkPersist :: MkPersistSettings -> [EntityDef] -> Q [Dec]
mkPersist mps ents' = do
    x <- fmap mconcat $ mapM (persistFieldFromEntity mps) ents
    y <- fmap mconcat $ mapM (mkEntity mps) ents
    z <- fmap mconcat $ mapM (mkJSON mps) ents
    return $ mconcat [x, y, z]
  where
    ents = map fixEntityDef ents'

-- | Implement special preprocessing on EntityDef as necessary for 'mkPersist'.
-- For example, strip out any fields marked as MigrationOnly.
fixEntityDef :: EntityDef -> EntityDef
fixEntityDef ed =
    ed { entityFields = filter keepField $ entityFields ed }
  where
    keepField fd = "MigrationOnly" `notElem` fieldAttrs fd &&
                   "SafeToRemove" `notElem` fieldAttrs fd

-- | Settings to be passed to the 'mkPersist' function.
data MkPersistSettings = MkPersistSettings
    { mpsBackend :: Type
    -- ^ Which database backend we\'re using.
    --
    -- When generating data types, each type is given a generic version- which
    -- works with any backend- and a type synonym for the commonly used
    -- backend. This is where you specify that commonly used backend.
    , mpsGeneric :: Bool
    -- ^ Create generic types that can be used with multiple backends. Good for
    -- reusable code, but makes error messages harder to understand. Default:
    -- True.
    , mpsPrefixFields :: Bool
    -- ^ Prefix field names with the model name. Default: True.
    , mpsEntityJSON :: Maybe EntityJSON
    -- ^ Generate @ToJSON@/@FromJSON@ instances for each model types. If it's
    -- @Nothing@, no instances will be generated. Default:
    --
    -- @
    --  Just EntityJSON
    --      { entityToJSON = 'keyValueEntityToJSON
    --      , entityFromJSON = 'keyValueEntityFromJSON
    --      }
    -- @
    , mpsGenerateLenses :: !Bool
    -- ^ Instead of generating normal field accessors, generator lens-style accessors.
    --
    -- Default: False
    --
    -- Since 1.3.1
    }

data EntityJSON = EntityJSON
    { entityToJSON :: Name
    -- ^ Name of the @toJSON@ implementation for @Entity a@.
    , entityFromJSON :: Name
    -- ^ Name of the @fromJSON@ implementation for @Entity a@.
    }

-- | Create an @MkPersistSettings@ with default values.
mkPersistSettings :: Type -- ^ Value for 'mpsBackend'
                  -> MkPersistSettings
mkPersistSettings t = MkPersistSettings
    { mpsBackend = t
    , mpsGeneric = False
    , mpsPrefixFields = True
    , mpsEntityJSON = Just EntityJSON
        { entityToJSON = 'entityIdToJSON
        , entityFromJSON = 'entityIdFromJSON
        }
    , mpsGenerateLenses = False
    }

-- | Use the 'SqlPersist' backend.
sqlSettings :: MkPersistSettings
sqlSettings = mkPersistSettings $ ConT ''SqlBackend

-- | Same as 'sqlSettings'.
--
-- Since 1.1.1
sqlOnlySettings :: MkPersistSettings
sqlOnlySettings = sqlSettings
{-# DEPRECATED sqlOnlySettings "use sqlSettings" #-}

recNameNoUnderscore :: MkPersistSettings -> HaskellName -> HaskellName -> Text
recNameNoUnderscore mps dt f
  | mpsPrefixFields mps = lowerFirst (unHaskellName dt) ++ upperFirst ft
  | otherwise           = lowerFirst ft
  where ft = unHaskellName f

recName :: MkPersistSettings -> HaskellName -> HaskellName -> Text
recName mps dt f =
    addUnderscore $ recNameNoUnderscore mps dt f
  where
    addUnderscore
        | mpsGenerateLenses mps = ("_" ++)
        | otherwise = id

lowerFirst :: Text -> Text
lowerFirst t =
    case uncons t of
        Just (a, b) -> cons (toLower a) b
        Nothing -> t

upperFirst :: Text -> Text
upperFirst t =
    case uncons t of
        Just (a, b) -> cons (toUpper a) b
        Nothing -> t

dataTypeDec :: MkPersistSettings -> EntityDef -> Dec
dataTypeDec mps t =
    DataD [] nameFinal paramsFinal constrs
    $ map (mkName . unpack) $ entityDerives t
  where
    mkCol x fd@FieldDef {..} =
        (mkName $ unpack $ recName mps x fieldHaskell,
         if fieldStrict then IsStrict else NotStrict,
         maybeIdType mps fd Nothing Nothing
        )
    (nameFinal, paramsFinal)
        | mpsGeneric mps = (nameG, [PlainTV backend])
        | otherwise = (name, [])
    nameG = mkName $ unpack $ unHaskellName (entityHaskell t) ++ "Generic"
    name = mkName $ unpack $ unHaskellName $ entityHaskell t
    cols = map (mkCol $ entityHaskell t) $ entityFields t
    backend = backendName

    constrs
        | entitySum t = map sumCon $ entityFields t
        | otherwise = [RecC name cols]

    sumCon fd = NormalC
        (sumConstrName mps t fd)
        [(NotStrict, maybeIdType mps fd Nothing Nothing)]

sumConstrName :: MkPersistSettings -> EntityDef -> FieldDef -> Name
sumConstrName mps t FieldDef {..} = mkName $ unpack $ concat
    [ if mpsPrefixFields mps
        then unHaskellName $ entityHaskell t
        else ""
    , upperFirst $ unHaskellName fieldHaskell
    , "Sum"
    ]

uniqueTypeDec :: MkPersistSettings -> EntityDef -> Dec
uniqueTypeDec mps t =
    DataInstD [] ''Unique
        [genericDataType mps (entityHaskell t) backendT]
            (map (mkUnique mps t) $ entityUniques t)
            []

mkUnique :: MkPersistSettings -> EntityDef -> UniqueDef -> Con
mkUnique mps t (UniqueDef (HaskellName constr) _ fields attrs) =
    NormalC (mkName $ unpack constr) types
  where
    types = map (go . flip lookup3 (entityFields t))
          $ map (unHaskellName . fst) fields

    force = "!force" `elem` attrs

    go :: (FieldDef, IsNullable) -> (Strict, Type)
    go (_, Nullable _) | not force = error nullErrMsg
    go (fd, y) = (NotStrict, maybeIdType mps fd Nothing (Just y))

    lookup3 :: Text -> [FieldDef] -> (FieldDef, IsNullable)
    lookup3 s [] =
        error $ unpack $ "Column not found: " ++ s ++ " in unique " ++ constr
    lookup3 x (fd@FieldDef {..}:rest)
        | x == unHaskellName fieldHaskell = (fd, nullable fieldAttrs)
        | otherwise = lookup3 x rest

    nullErrMsg =
      mconcat [ "Error:  By default we disallow NULLables in an uniqueness "
              , "constraint.  The semantics of how NULL interacts with those "
              , "constraints is non-trivial:  two NULL values are not "
              , "considered equal for the purposes of an uniqueness "
              , "constraint.  If you understand this feature, it is possible "
              , "to use it your advantage.    *** Use a \"!force\" attribute "
              , "on the end of the line that defines your uniqueness "
              , "constraint in order to disable this check. ***" ]

maybeIdType :: MkPersistSettings
           -> FieldDef
           -> Maybe Name -- ^ backend
           -> Maybe IsNullable
           -> Type
maybeIdType mps fd mbackend mnull = maybeTyp mayNullable idtyp
  where
    mayNullable = case mnull of
        (Just (Nullable ByMaybeAttr)) -> True
        _ -> maybeNullable fd
    idtyp = idType mps fd mbackend

backendDataType :: MkPersistSettings -> Type
backendDataType mps
    | mpsGeneric mps = backendT
    | otherwise = mpsBackend mps

genericDataType :: MkPersistSettings
                -> HaskellName -- ^ entity name
                -> Type -- ^ backend
                -> Type
genericDataType mps (HaskellName typ') backend
    | mpsGeneric mps = ConT (mkName $ unpack $ typ' ++ "Generic") `AppT` backend
    | otherwise = ConT $ mkName $ unpack typ'

idType :: MkPersistSettings -> FieldDef -> Maybe Name -> Type
idType mps fd mbackend =
    case foreignReference fd of
        Just typ ->
            ConT ''Key
            `AppT` genericDataType mps typ (VarT $ fromMaybe backendName mbackend)
        Nothing -> ftToType $ fieldType fd

degen :: [Clause] -> [Clause]
degen [] =
    let err = VarE 'error `AppE` LitE (StringL
                "Degenerate case, should never happen")
     in [normalClause [WildP] err]
degen x = x

mkToPersistFields :: MkPersistSettings -> String -> EntityDef -> Q Dec
mkToPersistFields mps constr ed@EntityDef { entitySum = isSum, entityFields = fields } = do
    clauses <-
        if isSum
            then sequence $ zipWith goSum fields [1..]
            else fmap return go
    return $ FunD 'toPersistFields clauses
  where
    go :: Q Clause
    go = do
        xs <- sequence $ replicate fieldCount $ newName "x"
        let pat = ConP (mkName constr) $ map VarP xs
        sp <- [|SomePersistField|]
        let bod = ListE $ map (AppE sp . VarE) xs
        return $ normalClause [pat] bod

    fieldCount = length fields

    goSum :: FieldDef -> Int -> Q Clause
    goSum fd idx = do
        let name = sumConstrName mps ed fd
        enull <- [|SomePersistField PersistNull|]
        let beforeCount = idx - 1
            afterCount = fieldCount - idx
            before = replicate beforeCount enull
            after = replicate afterCount enull
        x <- newName "x"
        sp <- [|SomePersistField|]
        let body = ListE $ mconcat
                [ before
                , [sp `AppE` VarE x]
                , after
                ]
        return $ normalClause [ConP name [VarP x]] body


mkToFieldNames :: [UniqueDef] -> Q Dec
mkToFieldNames pairs = do
    pairs' <- mapM go pairs
    return $ FunD 'persistUniqueToFieldNames $ degen pairs'
  where
    go (UniqueDef constr _ names _) = do
        names' <- lift names
        return $
            normalClause
                [RecP (mkName $ unpack $ unHaskellName constr) []]
                names'

mkUniqueToValues :: [UniqueDef] -> Q Dec
mkUniqueToValues pairs = do
    pairs' <- mapM go pairs
    return $ FunD 'persistUniqueToValues $ degen pairs'
  where
    go :: UniqueDef -> Q Clause
    go (UniqueDef constr _ names _) = do
        xs <- mapM (const $ newName "x") names
        let pat = ConP (mkName $ unpack $ unHaskellName constr) $ map VarP xs
        tpv <- [|toPersistValue|]
        let bod = ListE $ map (AppE tpv . VarE) xs
        return $ normalClause [pat] bod

isNotNull :: PersistValue -> Bool
isNotNull PersistNull = False
isNotNull _ = True

mapLeft :: (a -> c) -> Either a b -> Either c b
mapLeft _ (Right r) = Right r
mapLeft f (Left l)  = Left (f l)

fieldError :: Text -> Text -> Text
fieldError fieldName err = "field " `mappend` fieldName `mappend` ": " `mappend` err

mkFromPersistValues :: MkPersistSettings -> EntityDef -> Q [Clause]
mkFromPersistValues _ t@(EntityDef { entitySum = False }) =
    fromValues t "fromPersistValues" entE $ entityFields t
  where
    entE = ConE $ mkName $ unpack entName
    entName = unHaskellName $ entityHaskell t

mkFromPersistValues mps t@(EntityDef { entitySum = True }) = do
    nothing <- [|Left ("Invalid fromPersistValues input: sum type with all nulls. Entity: " `mappend` entName)|]
    clauses <- mkClauses [] $ entityFields t
    return $ clauses `mappend` [normalClause [WildP] nothing]
  where
    entName = unHaskellName $ entityHaskell t
    mkClauses _ [] = return []
    mkClauses before (field:after) = do
        x <- newName "x"
        let null' = ConP 'PersistNull []
            pat = ListP $ mconcat
                [ map (const null') before
                , [VarP x]
                , map (const null') after
                ]
            constr = ConE $ sumConstrName mps t field
        fs <- [|fromPersistValue $(return $ VarE x)|]
        let guard' = NormalG $ VarE 'isNotNull `AppE` VarE x
        let clause = Clause [pat] (GuardedB [(guard', InfixE (Just constr) fmapE (Just fs))]) []
        clauses <- mkClauses (field : before) after
        return $ clause : clauses

type Lens s t a b = forall f. Functor f => (a -> f b) -> s -> f t

lensPTH :: (s -> a) -> (s -> b -> t) -> Lens s t a b
lensPTH sa sbt afb s = fmap (sbt s) (afb $ sa s)

fmapE :: Exp
fmapE = VarE 'fmap

mkLensClauses :: MkPersistSettings -> EntityDef -> Q [Clause]
mkLensClauses mps t = do
    lens' <- [|lensPTH|]
    getId <- [|entityKey|]
    setId <- [|\(Entity _ value) key -> Entity key value|]
    getVal <- [|entityVal|]
    dot <- [|(.)|]
    keyVar <- newName "key"
    valName <- newName "value"
    xName <- newName "x"
    let idClause = normalClause
            [ConP (keyIdName t) []]
            (lens' `AppE` getId `AppE` setId)
    if entitySum t
        then return $ idClause : map (toSumClause lens' keyVar valName xName) (entityFields t)
        else return $ idClause : map (toClause lens' getVal dot keyVar valName xName) (entityFields t)
  where
    toClause lens' getVal dot keyVar valName xName f = normalClause
        [ConP (filterConName mps t f) []]
        (lens' `AppE` getter `AppE` setter)
      where
        fieldName = mkName $ unpack $ recName mps (entityHaskell t) (fieldHaskell f)
        getter = InfixE (Just $ VarE fieldName) dot (Just getVal)
        setter = LamE
            [ ConP 'Entity [VarP keyVar, VarP valName]
            , VarP xName
            ]
            $ ConE 'Entity `AppE` VarE keyVar `AppE` RecUpdE
                (VarE valName)
                [(fieldName, VarE xName)]

    toSumClause lens' keyVar valName xName f = normalClause
        [ConP (filterConName mps t f) []]
        (lens' `AppE` getter `AppE` setter)
      where
        emptyMatch = Match WildP (NormalB $ VarE 'error `AppE` LitE (StringL "Tried to use fieldLens on a Sum type")) []
        getter = LamE
            [ ConP 'Entity [WildP, VarP valName]
            ] $ CaseE (VarE valName)
            $ Match (ConP (sumConstrName mps t f) [VarP xName]) (NormalB $ VarE xName) []

            -- FIXME It would be nice if the types expressed that the Field is
            -- a sum type and therefore could result in Maybe.
            : if length (entityFields t) > 1 then [emptyMatch] else []
        setter = LamE
            [ ConP 'Entity [VarP keyVar, WildP]
            , VarP xName
            ]
            $ ConE 'Entity `AppE` VarE keyVar `AppE` (ConE (sumConstrName mps t f) `AppE` VarE xName)



-- | declare the key type and associated instances
-- a PathPiece instance is only generated for a Key with one field
mkKeyTypeDec :: MkPersistSettings -> EntityDef -> Q (Dec, [Dec])
mkKeyTypeDec mps t = do
    (instDecs, i) <-
      if mpsGeneric mps
        then if not useNewtype
               then do pfDec <- pfInstD
                       return (pfDec, [''Generic])
               else do gi <- genericNewtypeInstances
                       return (gi, [])
        else if not useNewtype
               then do pfDec <- pfInstD
                       return (pfDec, [''Show, ''Read, ''Eq, ''Ord, ''Generic])
                else do
                    let allInstances = [''Show, ''Read, ''Eq, ''Ord, ''PathPiece, ''PersistField, ''PersistFieldSql, ''ToJSON, ''FromJSON]
                    if customKeyType
                      then return ([], allInstances)
                      else do
                        bi <- backendKeyI
                        return (bi, allInstances)

    let kd = if useNewtype
               then NewtypeInstD [] k [recordType] dec i
               else DataInstD    [] k [recordType] [dec] i
    return (kd, instDecs)
  where
    keyConE = keyConExp t
    unKeyE = unKeyExp t
    dec = RecC (keyConName t) keyFields
    k = ''Key
    recordType = genericDataType mps (entityHaskell t) backendT
    pfInstD = -- FIXME: generate a PersistMap instead of PersistList
      [d|instance PersistField (Key $(pure recordType)) where
            toPersistValue = PersistList . keyToValues
            fromPersistValue (PersistList l) = keyFromValues l
            fromPersistValue got = error $ "fromPersistValue: expected PersistList, got: " `mappend` show got
         instance PersistFieldSql (Key $(pure recordType)) where
            sqlType _ = SqlString
         instance ToJSON (Key $(pure recordType))
         instance FromJSON (Key $(pure recordType))
      |]

    keyStringL = StringL . keyString
    -- ghc 7.6 cannot parse the left arrow Ident $() <- lexP
    keyPattern = BindS (ConP 'Ident [LitP $ keyStringL t])

    backendKeyGenericI =
        [d| instance PersistStore $(pure backendT) =>
              ToBackendKey $(pure backendT) $(pure recordType) where
                toBackendKey   = $(return unKeyE)
                fromBackendKey = $(return keyConE)
        |]
    backendKeyI = let bdt = backendDataType mps in
        [d| instance ToBackendKey $(pure bdt) $(pure recordType) where
                toBackendKey   = $(return unKeyE)
                fromBackendKey = $(return keyConE)
        |]

    -- truly unfortunate that TH doesn't support standalone deriving
    -- https://ghc.haskell.org/trac/ghc/ticket/8100
    genericNewtypeInstances = do
      instances <- [|lexP|] >>= \lexPE -> [| step readPrec >>= return . ($(pure keyConE) )|] >>= \readE -> do
        alwaysInstances <-
          [d|instance Show (BackendKey $(pure backendT)) => Show (Key $(pure recordType)) where
              showsPrec i x = showParen (i > app_prec) $
                (showString $ $(pure $ LitE $ keyStringL t) `mappend` " ") .
                showsPrec i ($(return unKeyE) x)
                where app_prec = (10::Int)
             instance Read (BackendKey $(pure backendT)) => Read (Key $(pure recordType)) where
                readPrec = parens $ (prec app_prec $ $(pure $ DoE [keyPattern lexPE, NoBindS readE]))
                  where app_prec = (10::Int)
             instance Eq (BackendKey $(pure backendT)) => Eq (Key $(pure recordType)) where
                x == y =
                    ($(return unKeyE) x) ==
                    ($(return unKeyE) y)
                x /= y =
                    ($(return unKeyE) x) ==
                    ($(return unKeyE) y)
             instance Ord (BackendKey $(pure backendT)) => Ord (Key $(pure recordType)) where
                compare x y = compare
                    ($(return unKeyE) x)
                    ($(return unKeyE) y)
             instance PathPiece (BackendKey $(pure backendT)) => PathPiece (Key $(pure recordType)) where
                toPathPiece = toPathPiece . $(return unKeyE)
                fromPathPiece = fmap $(return keyConE) . fromPathPiece
             instance PersistField (BackendKey $(pure backendT)) => PersistField (Key $(pure recordType)) where
                toPersistValue = toPersistValue . $(return unKeyE)
                fromPersistValue = fmap $(return keyConE) . fromPersistValue
             instance PersistFieldSql (BackendKey $(pure backendT)) => PersistFieldSql (Key $(pure recordType)) where
                sqlType = sqlType . fmap $(return unKeyE)
             instance ToJSON (BackendKey $(pure backendT)) => ToJSON (Key $(pure recordType)) where
                toJSON = toJSON . $(return unKeyE)
             instance FromJSON (BackendKey $(pure backendT)) => FromJSON (Key $(pure recordType)) where
                parseJSON = fmap $(return keyConE) . parseJSON
              |]

        if customKeyType then return alwaysInstances
          else fmap (alwaysInstances `mappend`) backendKeyGenericI
      return instances

    useNewtype = length keyFields < 2
    defaultIdType = fieldType (entityId t) == FTTypeCon Nothing (keyIdText t)
    keyFields = case entityPrimary t of
      Just pdef -> map primaryKeyVar $ (compositeFields pdef)
      Nothing   -> if defaultIdType
        then [idKeyVar backendKeyType]
        else [idKeyVar $ ftToType $ fieldType $ entityId t]
    customKeyType = not defaultIdType || not useNewtype || isJust (entityPrimary t)

    primaryKeyVar fd = (keyFieldName t fd, NotStrict, ftToType $ fieldType fd)
    idKeyVar ft = (unKeyName t, NotStrict, ft)

    backendKeyType
        | mpsGeneric mps = ConT ''BackendKey `AppT` backendT
        | otherwise      = ConT ''BackendKey `AppT` mpsBackend mps

keyIdName :: EntityDef -> Name
keyIdName = mkName . unpack . keyIdText

keyIdText :: EntityDef -> Text
keyIdText t = (unHaskellName $ entityHaskell t) `mappend` "Id"

unKeyName :: EntityDef -> Name
unKeyName t = mkName $ "un" `mappend` keyString t

unKeyExp :: EntityDef -> Exp
unKeyExp = VarE . unKeyName

backendT :: Type
backendT = VarT backendName

backendName :: Name
backendName = mkName "backend"

keyConName :: EntityDef -> Name
keyConName t = mkName $ resolveConflict $ keyString t
  where
    resolveConflict kn = if conflict then kn `mappend` "'" else kn
    conflict = any ((== HaskellName "key") . fieldHaskell) $ entityFields t

keyConExp :: EntityDef -> Exp
keyConExp = ConE . keyConName

keyString :: EntityDef -> String
keyString = unpack . keyText

keyText :: EntityDef -> Text
keyText t = unHaskellName (entityHaskell t) ++ "Key"

keyFieldName :: EntityDef -> FieldDef -> Name
keyFieldName t fd = mkName $ unpack $ lowerFirst (keyText t) `mappend` (unHaskellName $ fieldHaskell fd)

mkKeyToValues :: MkPersistSettings -> EntityDef -> Q Dec
mkKeyToValues _mps t = do
    (p, e) <- case entityPrimary t of
        Nothing  ->
          ([],) <$> [|(:[]) . toPersistValue . $(return $ unKeyExp t)|]
        Just pdef ->
          return $ toValuesPrimary pdef
    return $ FunD 'keyToValues $ return $ normalClause p e
  where
    toValuesPrimary pdef =
      ( [VarP recordName]
      , ListE $ map (\fd -> VarE 'toPersistValue `AppE` (VarE (keyFieldName t fd) `AppE` VarE recordName)) $ compositeFields pdef
      )
    recordName = mkName "record"

normalClause :: [Pat] -> Exp -> Clause
normalClause p e = Clause p (NormalB e) []

mkKeyFromValues :: MkPersistSettings -> EntityDef -> Q Dec
mkKeyFromValues _mps t = do
    clauses <- case entityPrimary t of
        Nothing  -> do
            e <- [|fmap $(return $ keyConE) . fromPersistValue . headNote|]
            return $ [normalClause [] e]
        Just pdef ->
            fromValues t "keyFromValues" keyConE (compositeFields pdef)
    return $ FunD 'keyFromValues clauses
  where
    keyConE = keyConExp t

headNote :: [PersistValue] -> PersistValue
headNote (x:[]) = x
headNote xs = error $ "mkKeyFromValues: expected a list of one element, got: "
  `mappend` show xs


fromValues :: EntityDef -> Text -> Exp -> [FieldDef] -> Q [Clause]
fromValues t funName conE fields = do
    x <- newName "x"
    let funMsg = entityText t `mappend` ": " `mappend` funName `mappend` " failed on: "
    patternMatchFailure <-
      [|Left $ mappend funMsg (pack $ show $(return $ VarE x))|]
    suc <- patternSuccess fields
    return [ suc, normalClause [VarP x] patternMatchFailure ]
  where
    patternSuccess [] = do
      rightE <- [|Right|]
      return $ normalClause [ListP []] (rightE `AppE` conE)
    patternSuccess fieldsNE = do
        x1 <- newName "x1"
        restNames <- mapM (\i -> newName $ "x" `mappend` show i) [2..length fieldsNE]
        (fpv1:mkPersistValues) <- mapM mkPvFromFd fieldsNE
        app1E <- [|(<$>)|]
        let conApp = infixFromPersistValue app1E fpv1 conE x1
        applyE <- [|(<*>)|]
        let applyFromPersistValue = infixFromPersistValue applyE

        return $ normalClause
            [ListP $ map VarP (x1:restNames)]
            (foldl' (\exp (name, fpv) -> applyFromPersistValue fpv exp name) conApp (zip restNames mkPersistValues))
        where
          infixFromPersistValue applyE fpv exp name =
              UInfixE exp applyE (fpv `AppE` VarE name)
          mkPvFromFd = mkPersistValue . unHaskellName . fieldHaskell
          mkPersistValue fieldName = [|mapLeft (fieldError fieldName) . fromPersistValue|]


mkEntity :: MkPersistSettings -> EntityDef -> Q [Dec]
mkEntity mps t = do
    t' <- lift t
    let nameT = unHaskellName entName
    let nameS = unpack nameT
    let clazz = ConT ''PersistEntity `AppT` genericDataType mps entName backendT
    tpf <- mkToPersistFields mps nameS t
    fpv <- mkFromPersistValues mps t
    utv <- mkUniqueToValues $ entityUniques t
    puk <- mkUniqueKeys t
    fkc <- mapM (mkForeignKeysComposite mps t) $ entityForeigns t

    let primaryField = entityId t
    
    fields <- mapM (mkField mps t) $ primaryField : entityFields t
    toFieldNames <- mkToFieldNames $ entityUniques t

    (keyTypeDec, keyInstanceDecs) <- mkKeyTypeDec mps t
    keyToValues' <- mkKeyToValues mps t
    keyFromValues' <- mkKeyFromValues mps t

    let addSyn -- FIXME maybe remove this
            | mpsGeneric mps = (:) $
                TySynD (mkName nameS) [] $
                    genericDataType mps entName $ mpsBackend mps
            | otherwise = id

    lensClauses <- mkLensClauses mps t

    lenses <- mkLenses mps t
    let instanceConstraint = if not (mpsGeneric mps) then [] else
          [mkClassP ''PersistStore [backendT]]

    return $ addSyn $
       dataTypeDec mps t : mconcat fkc `mappend`
      ([ TySynD (keyIdName t) [] $
            ConT ''Key `AppT` ConT (mkName nameS)
      , InstanceD instanceConstraint clazz $
        [ uniqueTypeDec mps t
        , keyTypeDec
        , keyToValues'
        , keyFromValues'
        , FunD 'entityDef [normalClause [WildP] t']
        , tpf
        , FunD 'fromPersistValues fpv
        , toFieldNames
        , utv
        , puk
        , DataInstD
            []
            ''EntityField
            [ genDataType
            , VarT $ mkName "typ"
            ]
            (map fst fields)
            []
        , FunD 'persistFieldDef (map snd fields)
        , TySynInstD
            ''PersistEntityBackend
#if MIN_VERSION_template_haskell(2,9,0)
            (TySynEqn
               [genDataType]
               (backendDataType mps))
#else
            [genDataType]
            (backendDataType mps)
#endif
        , FunD 'persistIdField [normalClause [] (ConE $ keyIdName t)]
        , FunD 'fieldLens lensClauses
        ]
      ] `mappend` lenses) `mappend` keyInstanceDecs
  where
    genDataType = genericDataType mps entName backendT
    entName = entityHaskell t

entityText :: EntityDef -> Text
entityText = unHaskellName . entityHaskell

mkLenses :: MkPersistSettings -> EntityDef -> Q [Dec]
mkLenses mps _ | not (mpsGenerateLenses mps) = return []
mkLenses _ ent | entitySum ent = return []
mkLenses mps ent = fmap mconcat $ forM (entityFields ent) $ \field -> do
    let lensName' = recNameNoUnderscore mps (entityHaskell ent) (fieldHaskell field)
        lensName = mkName $ unpack lensName'
        fieldName = mkName $ unpack $ "_" ++ lensName'
    needleN <- newName "needle"
    setterN <- newName "setter"
    fN <- newName "f"
    aN <- newName "a"
    yN <- newName "y"
    let needle = VarE needleN
        setter = VarE setterN
        f = VarE fN
        a = VarE aN
        y = VarE yN
        fT = mkName "f"
        -- FIXME if we want to get really fancy, then: if this field is the
        -- *only* Id field present, then set backend1 and backend2 to different
        -- values
        backend1 = backendName
        backend2 = backendName
        aT = maybeIdType mps field (Just backend1) Nothing
        bT = maybeIdType mps field (Just backend2) Nothing
        mkST backend = genericDataType mps (entityHaskell ent) (VarT backend)
        sT = mkST backend1
        tT = mkST backend2
        t1 `arrow` t2 = ArrowT `AppT` t1 `AppT` t2
        vars = PlainTV fT
             : (if mpsGeneric mps then [PlainTV backend1{-, PlainTV backend2-}] else [])
    return
        [ SigD lensName $ ForallT vars [mkClassP ''Functor [VarT fT]] $
            (aT `arrow` (VarT fT `AppT` bT)) `arrow`
            (sT `arrow` (VarT fT `AppT` tT))
        , FunD lensName $ return $ Clause
            [VarP fN, VarP aN]
            (NormalB $ fmapE
                `AppE` setter
                `AppE` (f `AppE` needle))
            [ FunD needleN [normalClause [] (VarE fieldName `AppE` a)]
            , FunD setterN $ return $ normalClause
                [VarP yN]
                (RecUpdE a
                    [ (fieldName, y)
                    ])
            ]
        ]

mkForeignKeysComposite :: MkPersistSettings -> EntityDef -> ForeignDef -> Q [Dec]
mkForeignKeysComposite mps t ForeignDef {..} = do
   let fieldName f = mkName $ unpack $ recName mps (entityHaskell t) f
   let fname = fieldName foreignConstraintNameHaskell
   let reftableString = unpack $ unHaskellName $ foreignRefTableHaskell
   let reftableKeyName = mkName $ reftableString `mappend` "Key"
   let tablename = mkName $ unpack $ entityText t
   recordName <- newName "record"
   
   let fldsE = map (\((foreignName, _),_) -> VarE (fieldName $ foreignName)
                 `AppE` VarE recordName) foreignFields
   let mkKeyE = foldl' AppE (maybeExp foreignNullable $ ConE reftableKeyName) fldsE
   let fn = FunD fname [normalClause [VarP recordName] mkKeyE]
   
   let t2 = maybeTyp foreignNullable $ ConT ''Key `AppT` ConT (mkName reftableString)
   let sig = SigD fname $ (ArrowT `AppT` (ConT tablename)) `AppT` t2
   return [sig, fn]

maybeExp :: Bool -> Exp -> Exp
maybeExp may exp | may = fmapE `AppE` exp
                 | otherwise = exp
maybeTyp :: Bool -> Type -> Type
maybeTyp may typ | may = ConT ''Maybe `AppT` typ
                 | otherwise = typ



-- | produce code similar to the following:
--
-- @
--   instance PersistEntity e => PersistField e where
--      toPersistValue = PersistMap $ zip columNames (map toPersistValue . toPersistFields)
--      fromPersistValue (PersistMap o) = 
--          let columns = HM.fromList o
--          in fromPersistValues $ map (\name ->
--            case HM.lookup name columns of
--              Just v -> v
--              Nothing -> PersistNull
--      fromPersistValue x = Left $ "Expected PersistMap, received: " ++ show x
--      sqlType _ = SqlString
-- @
persistFieldFromEntity :: MkPersistSettings -> EntityDef -> Q [Dec]
persistFieldFromEntity mps e = do
    ss <- [|SqlString|]
    obj <- [|\ent -> PersistMap $ zip (map pack columnNames) (map toPersistValue $ toPersistFields ent)|]
    fpv <- [|\x -> let columns = HM.fromList x
                    in fromPersistValues $ map
                         (\(name) ->
                            case HM.lookup (pack name) columns of
                                Just v -> v
                                Nothing -> PersistNull)
                         $ columnNames
          |]

    compose <- [|(<=<)|]
    getPersistMap' <- [|getPersistMap|]
    return
        [ persistFieldInstanceD (mpsGeneric mps) typ
            [ FunD 'toPersistValue [ normalClause [] obj ]
            , FunD 'fromPersistValue
                [ normalClause [] (InfixE (Just fpv) compose $ Just getPersistMap')
                ]
            ]
        , persistFieldSqlInstanceD (mpsGeneric mps) typ
            [ sqlTypeFunD ss
            ]
        ]
    where
      typ = genericDataType mps (entityHaskell e) backendT
      entFields = entityFields e
      columnNames  = map (unpack . unHaskellName . fieldHaskell) entFields

-- | Apply the given list of functions to the same @EntityDef@s.
--
-- This function is useful for cases such as:
--
-- >>> share [mkSave "myDefs", mkPersist sqlSettings] [persistLowerCase|...|]
share :: [[EntityDef] -> Q [Dec]] -> [EntityDef] -> Q [Dec]
share fs x = fmap mconcat $ mapM ($ x) fs

-- | Save the @EntityDef@s passed in under the given name.
mkSave :: String -> [EntityDef] -> Q [Dec]
mkSave name' defs' = do
    let name = mkName name'
    defs <- lift defs'
    return [ SigD name $ ListT `AppT` ConT ''EntityDef
           , FunD name [normalClause [] defs]
           ]

data Dep = Dep
    { depTarget :: HaskellName
    , depSourceTable :: HaskellName
    , depSourceField :: HaskellName
    , depSourceNull  :: IsNullable
    }

-- | Generate a 'DeleteCascade' instance for the given @EntityDef@s.
mkDeleteCascade :: MkPersistSettings -> [EntityDef] -> Q [Dec]
mkDeleteCascade mps defs = do
    let deps = concatMap getDeps defs
    mapM (go deps) defs
  where
    getDeps :: EntityDef -> [Dep]
    getDeps def =
        concatMap getDeps' $ entityFields $ fixEntityDef def
      where
        getDeps' :: FieldDef -> [Dep]
        getDeps' field@FieldDef {..} =
            case foreignReference field of
                Just name ->
                     return Dep
                        { depTarget = name
                        , depSourceTable = entityHaskell def
                        , depSourceField = fieldHaskell
                        , depSourceNull  = nullable fieldAttrs
                        }
                Nothing -> []
    go :: [Dep] -> EntityDef -> Q Dec
    go allDeps EntityDef{entityHaskell = name} = do
        let deps = filter (\x -> depTarget x == name) allDeps
        key <- newName "key"
        let del = VarE 'delete
        let dcw = VarE 'deleteCascadeWhere
        just <- [|Just|]
        filt <- [|Filter|]
        eq <- [|Eq|]
        left <- [|Left|]
        let mkStmt :: Dep -> Stmt
            mkStmt dep = NoBindS
                $ dcw `AppE`
                  ListE
                    [ filt `AppE` ConE filtName
                           `AppE` (left `AppE` val (depSourceNull dep))
                           `AppE` eq
                    ]
              where
                filtName = filterConName' mps (depSourceTable dep) (depSourceField dep)
                val (Nullable ByMaybeAttr) = just `AppE` VarE key
                val _                      =             VarE key



        let stmts :: [Stmt]
            stmts = map mkStmt deps `mappend`
                    [NoBindS $ del `AppE` VarE key]

        let entityT = genericDataType mps name backendT

        return $
            InstanceD
            [ mkClassP ''PersistQuery [backendT]
            , mkEqualP (ConT ''PersistEntityBackend `AppT` entityT) backendT
            ]
            (ConT ''DeleteCascade `AppT` entityT `AppT` backendT)
            [ FunD 'deleteCascade
                [normalClause [VarP key] (DoE stmts)]
            ]

mkUniqueKeys :: EntityDef -> Q Dec
mkUniqueKeys def | entitySum def =
    return $ FunD 'persistUniqueKeys [normalClause [WildP] (ListE [])]
mkUniqueKeys def = do
    c <- clause
    return $ FunD 'persistUniqueKeys [c]
  where
    clause = do
        xs <- forM (entityFields def) $ \fd -> do
            let x = fieldHaskell fd
            x' <- newName $ '_' : unpack (unHaskellName x)
            return (x, x')
        let pcs = map (go xs) $ entityUniques def
        let pat = ConP
                (mkName $ unpack $ unHaskellName $ entityHaskell def)
                (map (VarP . snd) xs)
        return $ normalClause [pat] (ListE pcs)

    go :: [(HaskellName, Name)] -> UniqueDef -> Exp
    go xs (UniqueDef name _ cols _) =
        foldl' (go' xs) (ConE (mkName $ unpack $ unHaskellName name)) (map fst cols)

    go' :: [(HaskellName, Name)] -> Exp -> HaskellName -> Exp
    go' xs front col =
        let Just col' = lookup col xs
         in front `AppE` VarE col'

sqlTypeFunD :: Exp -> Dec
sqlTypeFunD st = FunD 'sqlType
                [ normalClause [WildP] st ]

typeInstanceD :: Name
              -> Bool -- ^ include PersistStore backend constraint
              -> Type -> [Dec] -> Dec
typeInstanceD clazz hasBackend typ =
    InstanceD ctx (ConT clazz `AppT` typ)
  where
    ctx
        | hasBackend = [mkClassP ''PersistStore [backendT]]
        | otherwise = []

persistFieldInstanceD :: Bool -- ^ include PersistStore backend constraint
                      -> Type -> [Dec] -> Dec
persistFieldInstanceD = typeInstanceD ''PersistField

persistFieldSqlInstanceD :: Bool -- ^ include PersistStore backend constraint
                         -> Type -> [Dec] -> Dec
persistFieldSqlInstanceD = typeInstanceD ''PersistFieldSql

-- | Automatically creates a valid 'PersistField' instance for any datatype
-- that has valid 'Show' and 'Read' instances. Can be very convenient for
-- 'Enum' types.
derivePersistField :: String -> Q [Dec]
derivePersistField s = do
    ss <- [|SqlString|]
    tpv <- [|PersistText . pack . show|]
    fpv <- [|\dt v ->
                case fromPersistValue v of
                    Left e -> Left e
                    Right s' ->
                        case reads $ unpack s' of
                            (x, _):_ -> Right x
                            [] -> Left $ pack "Invalid " ++ pack dt ++ pack ": " ++ s'|]
    return
        [ persistFieldInstanceD False (ConT $ mkName s)
            [ FunD 'toPersistValue
                [ normalClause [] tpv
                ]
            , FunD 'fromPersistValue
                [ normalClause [] (fpv `AppE` LitE (StringL s))
                ]
            ]
        , persistFieldSqlInstanceD False (ConT $ mkName s)
            [ sqlTypeFunD ss
            ]
        ]

-- | Automatically creates a valid 'PersistField' instance for any datatype
-- that has valid 'ToJSON' and 'FromJSON' instances. For a datatype @T@ it
-- generates instances similar to these:
-- 
-- @
--    instance PersistField T where
--        toPersistValue = PersistByteString . L.toStrict . encode
--        fromPersistValue = (left T.pack) . eitherDecodeStrict' <=< fromPersistValue
--    instance PersistFieldSql T where
--        sqlType _ = SqlString
-- @
derivePersistFieldJSON :: String -> Q [Dec]
derivePersistFieldJSON s = do
    ss <- [|SqlString|]
    tpv <- [|PersistText . toJsonText|]
    fpv <- [|\dt v -> do
                text <- fromPersistValue v
                let bs' = TE.encodeUtf8 text
                case eitherDecodeStrict' bs' of
                    Left e -> Left $ pack "JSON decoding error for " ++ pack dt ++ pack ": " ++ pack e ++ pack ". On Input: " ++ decodeUtf8 bs'
                    Right x -> Right x|]
    return
        [ persistFieldInstanceD False (ConT $ mkName s)
            [ FunD 'toPersistValue
                [ normalClause [] tpv
                ]
            , FunD 'fromPersistValue
                [ normalClause [] (fpv `AppE` LitE (StringL s))
                ]
            ]
        , persistFieldSqlInstanceD False (ConT $ mkName s)
            [ sqlTypeFunD ss
            ]
        ]

-- | Creates a single function to perform all migrations for the entities
-- defined here. One thing to be aware of is dependencies: if you have entities
-- with foreign references, make sure to place those definitions after the
-- entities they reference.
mkMigrate :: String -> [EntityDef] -> Q [Dec]
mkMigrate fun allDefs = do
    body' <- body
    return
        [ SigD (mkName fun) typ
        , FunD (mkName fun) [normalClause [] body']
        ]
  where
    defs = filter isMigrated allDefs
    isMigrated def = not $ "no-migrate" `elem` entityAttrs def
    typ = ConT ''Migration
    body :: Q Exp
    body =
        case defs of
            [] -> [|return ()|]
            _  -> do
              defsName <- newName "defs"
              defsStmt <- do
                defs' <- mapM lift defs
                let defsExp = ListE defs'
                return $ LetS [ValD (VarP defsName) (NormalB defsExp) []]
              stmts <- mapM (toStmt $ VarE defsName) defs
              return (DoE $ defsStmt : stmts)
    toStmt :: Exp -> EntityDef -> Q Stmt
    toStmt defsExp ed = do
        u <- lift ed
        m <- [|migrate|]
        return $ NoBindS $ m `AppE` defsExp `AppE` u

instance Lift EntityDef where
    lift EntityDef{..} =
        [|EntityDef
            entityHaskell
            entityDB
            entityId
            entityAttrs
            entityFields
            entityUniques
            entityForeigns
            entityDerives
            entityExtra
            entitySum
            |]
instance Lift FieldDef where
    lift (FieldDef a b c d e f g) = [|FieldDef a b c d e f g|]
instance Lift UniqueDef where
    lift (UniqueDef a b c d) = [|UniqueDef a b c d|]
instance Lift CompositeDef where
    lift (CompositeDef a b) = [|CompositeDef a b|]
instance Lift ForeignDef where
    lift (ForeignDef a b c d e f g) = [|ForeignDef a b c d e f g|]

-- | A hack to avoid orphans.
class Lift' a where
    lift' :: a -> Q Exp
instance Lift' Text where
    lift' = liftT
instance Lift' a => Lift' [a] where
    lift' xs = do { xs' <- mapM lift' xs; return (ListE xs') }
instance (Lift' k, Lift' v) => Lift' (M.Map k v) where
    lift' m = [|M.fromList $(fmap ListE $ mapM liftPair $ M.toList m)|]

-- auto-lifting, means instances are overlapping
instance Lift' a => Lift a where
    lift = lift'

packPTH :: String -> Text
packPTH = pack
#if !MIN_VERSION_text(0, 11, 2)
{-# NOINLINE packPTH #-}
#endif

liftT :: Text -> Q Exp
liftT t = [|packPTH $(lift (unpack t))|]

liftPair :: (Lift' k, Lift' v) => (k, v) -> Q Exp
liftPair (k, v) = [|($(lift' k), $(lift' v))|]

instance Lift HaskellName where
    lift (HaskellName t) = [|HaskellName t|]
instance Lift DBName where
    lift (DBName t) = [|DBName t|]
instance Lift FieldType where
    lift (FTTypeCon Nothing t)  = [|FTTypeCon Nothing t|]
    lift (FTTypeCon (Just x) t) = [|FTTypeCon (Just x) t|]
    lift (FTApp x y) = [|FTApp x y|]
    lift (FTList x) = [|FTList x|]

instance Lift PersistFilter where
    lift Eq = [|Eq|]
    lift Ne = [|Ne|]
    lift Gt = [|Gt|]
    lift Lt = [|Lt|]
    lift Ge = [|Ge|]
    lift Le = [|Le|]
    lift In = [|In|]
    lift NotIn = [|NotIn|]
    lift (BackendSpecificFilter x) = [|BackendSpecificFilter x|]

instance Lift PersistUpdate where
    lift Assign = [|Assign|]
    lift Add = [|Add|]
    lift Subtract = [|Subtract|]
    lift Multiply = [|Multiply|]
    lift Divide = [|Divide|]
    lift (BackendSpecificUpdate x) = [|BackendSpecificUpdate x|]

instance Lift SqlType where
    lift SqlString = [|SqlString|]
    lift SqlInt32 = [|SqlInt32|]
    lift SqlInt64 = [|SqlInt64|]
    lift SqlReal = [|SqlReal|]
    lift (SqlNumeric x y) =
        [|SqlNumeric (fromInteger x') (fromInteger y')|]
      where
        x' = fromIntegral x :: Integer
        y' = fromIntegral y :: Integer
    lift SqlBool = [|SqlBool|]
    lift SqlDay = [|SqlDay|]
    lift SqlTime = [|SqlTime|]
    lift SqlDayTime = [|SqlDayTime|]
    lift SqlBlob = [|SqlBlob|]
    lift (SqlOther a) = [|SqlOther a|]

-- Ent
--   fieldName FieldType
--
-- forall . typ ~ FieldType => EntFieldName
--
-- EntFieldName = FieldDef ....
mkField :: MkPersistSettings -> EntityDef -> FieldDef -> Q (Con, Clause)
mkField mps et cd = do
    let con = ForallC
                []
                [mkEqualP (VarT $ mkName "typ") $ maybeIdType mps cd Nothing Nothing]
                $ NormalC name []
    bod <- lift cd
    let cla = normalClause
                [ConP name []]
                bod
    return (con, cla)
  where
    name = filterConName mps et cd

maybeNullable :: FieldDef -> Bool
maybeNullable fd = nullable (fieldAttrs fd) == Nullable ByMaybeAttr

filterConName :: MkPersistSettings
              -> EntityDef
              -> FieldDef
              -> Name
filterConName mps entity field = filterConName' mps (entityHaskell entity) (fieldHaskell field)

filterConName' :: MkPersistSettings
               -> HaskellName -- ^ table
               -> HaskellName -- ^ field
               -> Name
filterConName' mps entity field = mkName $ unpack $ concat
    [ if mpsPrefixFields mps || field == HaskellName "Id"
        then unHaskellName entity
        else ""
    , upperFirst $ unHaskellName field
    ]

ftToType :: FieldType -> Type
ftToType (FTTypeCon Nothing t) = ConT $ mkName $ unpack t
ftToType (FTTypeCon (Just m) t) = ConT $ mkName $ unpack $ concat [m, ".", t]
ftToType (FTApp x y) = ftToType x `AppT` ftToType y
ftToType (FTList x) = ListT `AppT` ftToType x

infixr 5 ++
(++) :: Text -> Text -> Text
(++) = append

mkJSON :: MkPersistSettings -> EntityDef -> Q [Dec]
mkJSON _ def | not ("json" `elem` entityAttrs def) = return []
mkJSON mps def = do
    pureE <- [|pure|]
    apE' <- [|(<*>)|]
    packE <- [|pack|]
    dotEqualE <- [|(.=)|]
    dotColonE <- [|(.:)|]
    dotColonQE <- [|(.:?)|]
    objectE <- [|object|]
    obj <- newName "obj"
    mzeroE <- [|mzero|]

    xs <- mapM (newName . unpack . unHaskellName . fieldHaskell)
        $ entityFields def

    let conName = mkName $ unpack $ unHaskellName $ entityHaskell def
        typ = genericDataType mps (entityHaskell def) backendT
        toJSONI = typeInstanceD ''ToJSON (mpsGeneric mps) typ [toJSON']
        toJSON' = FunD 'toJSON $ return $ normalClause
            [ConP conName $ map VarP xs]
            (objectE `AppE` ListE pairs)
        pairs = zipWith toPair (entityFields def) xs
        toPair f x = InfixE
            (Just (packE `AppE` LitE (StringL $ unpack $ unHaskellName $ fieldHaskell f)))
            dotEqualE
            (Just $ VarE x)
        fromJSONI = typeInstanceD ''FromJSON (mpsGeneric mps) typ [parseJSON']
        parseJSON' = FunD 'parseJSON
            [ normalClause [ConP 'Object [VarP obj]]
                (foldl'
                    (\x y -> InfixE (Just x) apE' (Just y))
                    (pureE `AppE` ConE conName)
                    pulls
                )
            , normalClause [WildP] mzeroE
            ]
        pulls = map toPull $ entityFields def
        toPull f = InfixE
            (Just $ VarE obj)
            (if maybeNullable f then dotColonQE else dotColonE)
            (Just $ AppE packE $ LitE $ StringL $ unpack $ unHaskellName $ fieldHaskell f)
    case mpsEntityJSON mps of
        Nothing -> return [toJSONI, fromJSONI]
        Just entityJSON -> do
            entityJSONIs <- if mpsGeneric mps
              then [d|
#if MIN_VERSION_base(4, 6, 0)
                instance PersistStore backend => ToJSON (Entity $(pure typ)) where
                    toJSON = $(varE (entityToJSON entityJSON))
                instance PersistStore backend => FromJSON (Entity $(pure typ)) where
                    parseJSON = $(varE (entityFromJSON entityJSON))
#endif
                |]
              else [d|
                instance ToJSON (Entity $(pure typ)) where
                    toJSON = $(varE (entityToJSON entityJSON))
                instance FromJSON (Entity $(pure typ)) where
                    parseJSON = $(varE (entityFromJSON entityJSON))
                |]
            return $ toJSONI : fromJSONI : entityJSONIs

mkClassP :: Name -> [Type] -> Pred
#if MIN_VERSION_template_haskell(2,10,0)
mkClassP cla tys = foldl AppT (ConT cla) tys
#else
mkClassP = ClassP
#endif

mkEqualP :: Type -> Type -> Pred
#if MIN_VERSION_template_haskell(2,10,0)
mkEqualP tleft tright = foldl AppT EqualityT [tleft, tright]
#else
mkEqualP = EqualP
#endif

-- entityUpdates :: EntityDef -> [(HaskellName, FieldType, IsNullable, PersistUpdate)]
-- entityUpdates =
--     concatMap go . entityFields
--   where
--     go FieldDef {..} = map (\a -> (fieldHaskell, fieldType, nullable fieldAttrs, a)) [minBound..maxBound]

-- mkToUpdate :: String -> [(String, PersistUpdate)] -> Q Dec
-- mkToUpdate name pairs = do
--     pairs' <- mapM go pairs
--     return $ FunD (mkName name) $ degen pairs'
--   where
--     go (constr, pu) = do
--         pu' <- lift pu
--         return $ normalClause [RecP (mkName constr) []] pu'


-- mkToFieldName :: String -> [(String, String)] -> Dec
-- mkToFieldName func pairs =
--         FunD (mkName func) $ degen $ map go pairs
--   where
--     go (constr, name) =
--         normalClause [RecP (mkName constr) []] (LitE $ StringL name)

-- mkToValue :: String -> [String] -> Dec
-- mkToValue func = FunD (mkName func) . degen . map go
--   where
--     go constr =
--         let x = mkName "x"
--          in normalClause [ConP (mkName constr) [VarP x]]
--                    (VarE 'toPersistValue `AppE` VarE x)

