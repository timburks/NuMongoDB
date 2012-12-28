#import "NuMongoDB.h"
#import "NuBSON.h"

@interface NuBSON (Private)
- (NuBSON *) initWithBSON:(bson) b;
@end

@implementation NuMongoDBCursor

- (NuMongoDBCursor *) initWithCursor:(mongo_cursor *) c
{
    if (self = [super init]) {
        cursor = c;
    }
    return self;
}

- (mongo_cursor *) cursor
{
    return cursor;
}

- (BOOL) next
{
    return mongo_cursor_next(cursor);
}

- (bson) current
{
    return cursor->current;
}

- (NuBSON *) currentBSON
{
    return [[NuBSON alloc] initWithBSON:cursor->current];
}

- (NSDictionary *) currentObject
{
    return [[self currentBSON] dictionaryValue];
}

- (void) dealloc
{
    mongo_cursor_destroy(cursor);
    
}

- (NSMutableArray *) arrayValue
{
    NSMutableArray *result = [NSMutableArray array];
    while([self next]) {
        [result addObject:[self currentObject]];
    }
    return result;
}

- (NSMutableArray *) arrayValueWithLimit:(int) limit
{
    int count = 0;
    NSMutableArray *result = [NSMutableArray array];
    while([self next] && (count < limit)) {
        [result addObject:[self currentObject]];
        count++;
    }
    return result;
}

@end

@implementation NuMongoDB

static BOOL enableUpdateTimestamps = NO;

+ (void) setEnableUpdateTimestamps:(BOOL) enable {
	enableUpdateTimestamps = YES;
}

- (int) connectWithOptions:(NSDictionary *) options
{
	char *host = malloc(255);
	int port;
    id hostVal = options ? [options objectForKey:@"host"] : nil;
    if (hostVal) {
        strncpy(host, [hostVal cStringUsingEncoding:NSUTF8StringEncoding], 255);
        host[254] = '\0';
    }
    else {
        strncpy(host, "127.0.0.1", 255);
        host[254] = '\0';
    }
    id portVal = options ? [options objectForKey:@"port"] : nil;
    if (portVal) {
        port = [portVal intValue];
    }
    else {
        port = 27017;
    }
    //NSLog(@"connecting to host %s port %d", opts.host, opts.port);
	int status = mongo_client(conn, host, port);
	
    return status;
}

- (int) connect {
	return [self connectWithOptions:nil];
}

- (void) addUser:(NSString *) user withPassword:(NSString *) password forDatabase:(NSString *) database
{
    mongo_cmd_add_user(conn, [database cStringUsingEncoding:NSUTF8StringEncoding],
        [user cStringUsingEncoding:NSUTF8StringEncoding],
        [password cStringUsingEncoding:NSUTF8StringEncoding]);
}

- (BOOL) authenticateUser:(NSString *) user withPassword:(NSString *) password forDatabase:(NSString *) database
{
    return mongo_cmd_authenticate(conn, [database cStringUsingEncoding:NSUTF8StringEncoding],
        [user cStringUsingEncoding:NSUTF8StringEncoding],
        [password cStringUsingEncoding:NSUTF8StringEncoding]);
}

- (NuMongoDBCursor *) find:(id) query inCollection:(NSString *) collection
{
    bson *b = bson_for_object(query);
    mongo_cursor *cursor = mongo_find(conn, [collection cStringUsingEncoding:NSUTF8StringEncoding], b, 0, 0, 0, 0 );
    return [[NuMongoDBCursor alloc] initWithCursor:cursor];
}

- (NuMongoDBCursor *) find:(id) query inCollection:(NSString *) collection returningFields:(id) fields numberToReturn:(int) nToReturn numberToSkip:(int) nToSkip
{
    bson *b = bson_for_object(query);
    bson *f = bson_for_object(fields);
    mongo_cursor *cursor = mongo_find(conn, [collection cStringUsingEncoding:NSUTF8StringEncoding], b, f, nToReturn, nToSkip, 0 );
    return [[NuMongoDBCursor alloc] initWithCursor:cursor];
}

- (NSMutableArray *) findArray:(id) query inCollection:(NSString *) collection
{
    NuMongoDBCursor *cursor = [self find:query inCollection:collection];
    return [cursor arrayValue];
}

- (NSMutableArray *) findArray:(id) query inCollection:(NSString *) collection returningFields:(id) fields numberToReturn:(int) nToReturn numberToSkip:(int) nToSkip
{
    NuMongoDBCursor *cursor = [self find:query inCollection:collection returningFields:fields numberToReturn:nToReturn numberToSkip:nToSkip];
    return [cursor arrayValueWithLimit:nToReturn];
}

- (NSMutableDictionary *) findOne:(id) query inCollection:(NSString *) collection
{
    bson *b = bson_for_object(query);
    bson bsonResult;
    bson_bool_t result = (mongo_find_one(conn, [collection cStringUsingEncoding:NSUTF8StringEncoding], b, 0, &bsonResult) == MONGO_OK);
    return result ? [[[NuBSON alloc] initWithBSON:bsonResult]  dictionaryValue] : nil;
}

- (id) insertObject:(id) insert intoCollection:(NSString *) collection
{
    if (![insert objectForKey:@"_id"]) {
        insert = [insert mutableCopy];
        [insert setObject:[NuBSONObjectID objectID] forKey:@"_id"];
    }
	if (enableUpdateTimestamps) {
    	[insert setObject:[NSDate date] forKey:@"_up"];
	}
    bson *b = bson_for_object(insert);
    if (b) {
        mongo_insert(conn, [collection cStringUsingEncoding:NSUTF8StringEncoding], b, NULL);
        return [insert objectForKey:@"_id"];
    }
    else {
        NSLog(@"incomplete insert: insert must not be nil.");
        return nil;
    }
}

- (void) updateObject:(id) update inCollection:(NSString *) collection
withCondition:(id) condition insertIfNecessary:(BOOL) insertIfNecessary updateMultipleEntries:(BOOL) updateMultipleEntries
{
	if (enableUpdateTimestamps) {
    	[update setObject:[NSDate date] forKey:@"_up"];
	}
    bson *bupdate = bson_for_object(update);
    bson *bcondition = bson_for_object(condition);
    if (bupdate && bcondition) {
        mongo_update(conn, [collection cStringUsingEncoding:NSUTF8StringEncoding],
            bcondition,
            bupdate,
            (insertIfNecessary ? MONGO_UPDATE_UPSERT : 0) + (updateMultipleEntries ? MONGO_UPDATE_MULTI : 0), NULL);
    }
    else {
        NSLog(@"incomplete update: update and condition must not be nil.");
    }
}

- (void) removeWithCondition:(id) condition fromCollection:(NSString *) collection
{
    bson *bcondition = bson_for_object(condition);
    mongo_remove(conn, [collection cStringUsingEncoding:NSUTF8StringEncoding], bcondition, NULL);
}

- (int) countWithCondition:(id) condition inCollection:(NSString *) collection inDatabase:(NSString *) database
{
    bson *bcondition = bson_for_object(condition);
    return mongo_count(conn, [database cStringUsingEncoding:NSUTF8StringEncoding], [collection cStringUsingEncoding:NSUTF8StringEncoding], bcondition);
}

- (id) runCommand:(id) command inDatabase:(NSString *) database
{
    bson *bcommand = bson_for_object(command);
    bson bsonResult;
    bson_bool_t result = mongo_run_command(conn, [database cStringUsingEncoding:NSUTF8StringEncoding], bcommand, &bsonResult);
    return result ? [[[NuBSON alloc] initWithBSON:bsonResult]  dictionaryValue] : nil;
}

- (BOOL) dropDatabase:(NSString *) database
{
    return mongo_cmd_drop_db(conn, [database cStringUsingEncoding:NSUTF8StringEncoding]);
}

- (BOOL) dropCollection:(NSString *) collection inDatabase:(NSString *) database
{
    return mongo_cmd_drop_collection(conn,
        [database cStringUsingEncoding:NSUTF8StringEncoding],
        [collection cStringUsingEncoding:NSUTF8StringEncoding],
        NULL);
}

- (id) collectionNamesInDatabase:(NSString *) database
{
    NSArray *names = [self findArray:nil inCollection:[database stringByAppendingString:@".system.namespaces"]];
    NSMutableArray *result = [NSMutableArray array];
    for (int i = 0; i < [names count]; i++) {
        id name = [[[names objectAtIndex:i] objectForKey:@"name"]
            stringByReplacingOccurrencesOfString:[database stringByAppendingString:@"."]
            withString:@""];
        NSRange match = [name rangeOfString:@".$_id_"];
        if (match.location != NSNotFound) {
            continue;
        }
        match = [name rangeOfString:@"system.indexes"];
        if (match.location != NSNotFound) {
            continue;
        }
        [result addObject:name];
    }
    return result;
}

- (BOOL) ensureCollection:(NSString *) collection hasIndex:(NSObject *) key withOptions:(int) options
{
    bson output;
    return mongo_create_index(conn,
        [collection cStringUsingEncoding:NSUTF8StringEncoding],
        bson_for_object(key),
        options,
        &output);
}

- (void) close
{
    mongo_destroy(conn );
}

@end
