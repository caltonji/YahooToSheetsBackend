
# given leagues_db, a list of league objects, return leagues_df, a pandas dataframe, with a new column url
# if the leagues is in leagues_db
def merge_leagues_df_db(leagues_df, leagues_db):
    leagues_df["export_url"] = leagues_df.apply(lambda row: leagues_db[row["league_key"]]['url'] if row["league_key"] in leagues_db else None, axis=1)
    return leagues_df