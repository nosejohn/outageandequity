import delimited "/Users/johnkim/final_zipcode_analysis.csv", clear

*rename date_x date
*rename ppt_x ppt
*rename year_x year
*rename tmax_x tmax
*rename tmin_x tmin
*rename tmean_x tmean

*drop date_y ppt_y year_y tmax_y tmin_y tmean_y 





gen date_new = date(date, "YMD")

format date_new %td

xtset zipcode date_new
encode state, gen(state_encoded)

gen per_black = nh_black/total_population

gen per_hispanic = hispanic/total_population


xtset zipcode date_new


gen per_customers_affected = total_customers_affected / num_cus 

gen per_outage = outages_count / num_cus

gen per_customer_duration = customers_duration / num_cus 

gen per_female = female / total_population 

gen per_pop_customers_duration = customers_duration / total_population 

gen per_bachelor = bachelor / total_population 

gen per_old = age65_older /total_population 

areg outages_count ppt tmax state_encoded num_cus population_density per_black per_hispanic per_capita_income tree median_year_structure_built gini_index per_female, absorb(date) vce(cluster zipcode)


xtnbreg ln_per_customer_duration ppt tmax i.state_encoded  num_cus population_density per_black per_hispanic per_capita_income tree median_year_structure_built gini_index per_female, re vce(bootstrap)





