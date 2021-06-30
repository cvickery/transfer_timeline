-- Link all tables in cuny_curriculum to cuny_transfers.
-- This should actually be done using two schemas for one db, but I didn't do that, so this is a
-- temporary hack.
-- The wrapper persists, so this is more documentatary than procedural.
-- Actually, the wrapper was a bother, and I removed it. But this is sql is still of documentarty
-- value. Maybe.

create extension postgres_fdw;
create server cunycu foreign data wrapper postgres_fdw options (dbname 'cuny_curriculum');
create user mapping for vickery server cunycu;
import foreign schema public from server cunycu into public;
