def get_year_vector(year_vector, first_model_year, life_time,
                    duration_period_sum, first_tech_year, last_tech_year):
    dps = duration_period_sum
    first_vtg_year = dps[dps[first_model_year] < life_time][first_model_year]
    first_vtg_year = first_vtg_year.index[0]
    return [y for y in year_vector if (y >= first_vtg_year)
            and (y <= last_tech_year) and (y >= first_tech_year)]
