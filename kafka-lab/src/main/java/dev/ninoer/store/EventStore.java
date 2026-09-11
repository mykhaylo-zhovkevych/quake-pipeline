package dev.ninoer.store;

// TODO(part 3): one Connection, plain PreparedStatements.
//  insertLog(QuakeEvent)    -> INSERT ... ON CONFLICT (usgs_id, updated_at) DO NOTHING
//  upsertCurrent(QuakeEvent) -> INSERT ... ON CONFLICT (usgs_id) DO UPDATE SET ...
public class EventStore {


}
