import cvxpy as cp
import os
import pandas as pd
import numpy as np
import csv

class Hindsight_solution():
    def __init__(self, subnet):
        self.subnet = subnet
        self.build_and_solve_LP()
        self.save_hindsight_to_summary_file()

    def add_variables(self):
        self.y_rk = {}
        for k in range(len(self.subnet.K)):
            for r in self.subnet.R_k[k]:
                self.y_rk[(r,k)] = cp.Variable(integer=True)
        self.g_k = {}
        for k in range(len(self.subnet.K)):
            self.g_k[k] = cp.Variable(integer=True)

    def add_cap_constraints(self):
        for j in range(len(self.subnet.J)):
            self.constraints.append(sum(self.y_rk[(r,k)] for r in self.subnet.R_j[j] for k in self.subnet.K_r[r]) <= self.subnet.J[j].cap)

    def add_demand_constraints(self):
        for k in range(len(self.subnet.K)):
            self.constraints.append(self.g_k[k] + sum(self.y_rk[(r,k)] for r in self.subnet.R_k[k]) == self.subnet.K[k].units_from_shipment_data)
    
    def add_nonnegative_constraint(self):
        for k in range(len(self.subnet.K)):
            for r in self.subnet.R_k[k]:
                self.constraints.append(self.y_rk[(r,k)] >= 0)
        for k in range(len(self.subnet.K)):
            self.constraints.append(self.g_k[k] >= 0)

    def add_objective(self):
        self.variable_cost = sum(self.y_rk[(r,k)]*self.subnet.c_kr[(k,r)] for k in range(len(self.subnet.K))for r in self.subnet.R_k[k])
        self.panelty_cost = sum(self.g_k[k]*10000000 for k in range(len(self.subnet.K)))
        self.objective = cp.Minimize(self.variable_cost + self.panelty_cost)
    
    def save_commodity_related(self):
        flows_on_routes_all = []
        self.subnet.commodity_data['amzl_direct_hindsight_flow'] = 0
        self.subnet.commodity_data['amzl_indirect_hindsight_flow'] = 0
        self.subnet.commodity_data['ddu_hindsight_flow'] = 0
        self.subnet.commodity_data['3p_hindsight_flow'] = 0
        self.subnet.commodity_data['infeasible_hindsight_flow'] = 0
        for k in range(len(self.subnet.K)):
            self.subnet.commodity_data.loc[k,'infeasible_hindsight_flow'] = self.g_k[k].value
            for r in self.subnet.R_k[k]:
                if self.subnet.R[r].type == '3p':
                    self.subnet.commodity_data.loc[k,'3p_hindsight_flow'] += self.y_rk[(r,k)].value
                elif self.subnet.R[r].type == 'ddu':
                    self.subnet.commodity_data.loc[k,'ddu_hindsight_flow'] += self.y_rk[(r,k)].value
                elif self.subnet.R[r].type == 'amzl_direct':
                    self.subnet.commodity_data.loc[k,'amzl_direct_hindsight_flow'] += self.y_rk[(r,k)].value
                elif self.subnet.R[r].type == 'amzl_indirect':
                    self.subnet.commodity_data.loc[k,'amzl_indirect_hindsight_flow'] += self.y_rk[(r,k)].value
            flows_on_routes = {}
            for r in self.subnet.R_k[k]:
                flows_on_routes[self.subnet.R[r].index] = self.y_rk[(r,k)].value
            flows_on_routes_all.append(flows_on_routes)
        self.subnet.commodity_data['hindsight_route_assignment'] = np.array(flows_on_routes_all)

    def get_solution(self):
        for j in range(len(self.subnet.J)):
            self.subnet.J[j].hindsight_flow = sum((self.y_rk[(r,k)].value) for r in self.subnet.R_j[j] for k in self.subnet.K_r[r])

        self.subnet.resource_data['hindsight_flow'] = pd.Series([self.subnet.J[j].hindsight_flow for j in range(len(self.subnet.J))])
        self.subnet.resource_data['hindsight_remaining_cap'] = self.subnet.resource_data['cap'] - self.subnet.resource_data['hindsight_flow']
        self.save_commodity_related()

    def build_and_solve_LP(self):
        self.constraints = []
        self.add_variables()
        self.add_cap_constraints()
        self.add_demand_constraints()
        self.add_nonnegative_constraint()
        self.add_objective()
        self.LP = cp.Problem(self.objective, self.constraints)
        self.obj_value = (self.LP).solve(solver=cp.GUROBI)
        self.get_solution()
        
    def save_hindsight_to_summary_file(self):
        total_packages = sum(self.subnet.K[k].units_from_shipment_data for k in range(len(self.subnet.K)))
        self.subnet.commodity_data = self.subnet.commodity_data.rename(columns = {'AMZL_variable_cost':'amzl_variable_cost'})
        total_hindsight_cost = sum(self.y_rk[(r,k)].value*self.subnet.c_kr[(k,r)] for k in range(len(self.subnet.K))for r in self.subnet.R_k[k])
        self.infeasible_package = self.subnet.commodity_data['infeasible_hindsight_flow'].sum()
        self.per_package_cost = total_hindsight_cost/(total_packages - self.infeasible_package)
        self.subnet.summary_data['hindsight_per_package_cost'] = self.per_package_cost
        self.subnet.summary_data['hindsight_infeasible_packages'] = self.infeasible_package
        self.subnet.summary_data['hindsight_amzl_direct_percentage'] = self.subnet.commodity_data['amzl_direct_hindsight_flow'].sum()/total_packages*100
        self.subnet.summary_data['hindsight_amzl_indirect_percentage'] = self.subnet.commodity_data['amzl_indirect_hindsight_flow'].sum()/total_packages*100
        self.subnet.summary_data['hindsight_ddu_percentage'] = self.subnet.commodity_data['ddu_hindsight_flow'].sum()/total_packages*100
        self.subnet.summary_data['hindsight_3p_percentage'] = self.subnet.commodity_data['3p_hindsight_flow'].sum()/total_packages*100


class all():
    def __init__(self, subnet):
        self.subnet = subnet 
        self.assignments_by_package()
        self.true_costs()
        self.save_to_summary_file()


    def assignments_by_package(self):
        self.total_numb_packages = len(self.subnet.shipment_data)
        self.infeasible_package = len(self.subnet.shipment_data[self.subnet.shipment_data.assigned_route == 'no_feasible_route'])
        # add type to assignnment
        self.subnet.shipment_data = self.subnet.shipment_data.merge(self.subnet.route_data[['index','type']],left_on = 'assigned_route',right_on = 'index', how = 'left').drop(columns = 'index')
        self.subnet.shipment_data['type'] = self.subnet.shipment_data.type.fillna('no_feasible_route')

       
    def true_costs(self):
        self.subnet.shipment_data = self.subnet.shipment_data.merge(self.subnet.route_data[['index','base_cost']],left_on = 'assigned_route',right_on = 'index', how = 'left').drop(columns = 'index')
        self.true_cost = self.subnet.shipment_data.base_cost.sum()
        self.per_package_cost = self.true_cost/(self.total_numb_packages - self.infeasible_package)
        
    def save_to_summary_file(self):
        total_packages = len(self.subnet.shipment_data)
        self.subnet.summary_data['total_packages'] = total_packages
        self.subnet.summary_data['total_cost'] = self.true_cost 
        self.subnet.summary_data['per_package_cost'] = self.per_package_cost 
        self.subnet.summary_data['infeasible_packages'] = self.infeasible_package
        self.subnet.summary_data['amzl_direct_percentage'] = len(self.subnet.shipment_data[(self.subnet.shipment_data.type == 'amzl_direct')&(self.subnet.shipment_data.assigned_route != 'no_feasible_route')])/total_packages*100
        self.subnet.summary_data['amzl_indirect_percentage'] = len(self.subnet.shipment_data[(self.subnet.shipment_data.type == 'amzl_indirect')&(self.subnet.shipment_data.assigned_route != 'no_feasible_route')])/total_packages*100
        self.subnet.summary_data['ddu_percentage'] = len(self.subnet.shipment_data[(self.subnet.shipment_data.type == 'ddu')&(self.subnet.shipment_data.assigned_route != 'no_feasible_route')])/total_packages*100
        self.subnet.summary_data['3p_percentage'] = len(self.subnet.shipment_data[(self.subnet.shipment_data.type == '3p')&(self.subnet.shipment_data.assigned_route != 'no_feasible_route')])/total_packages*100

class save_final_states():
    def __init__(self, subnet, mode, OUTPUT_DIR_FINAL):
        self.digit = 3
        self.subnet = subnet
        self.mode = mode
        self.OUTPUT_DIR_FINAL = OUTPUT_DIR_FINAL
        self.save_assignment_file()
        self.save_resource_file()
        self.save_commodity_file()
        self.save_summary_file()
        
    def save_assignment_file(self):
        self.subnet.shipment_data.to_csv(os.path.join(self.OUTPUT_DIR_FINAL, 'assignments.csv'), index = False)

        self.subnet.shipment_data.resource_caps_of_assigned_route = self.subnet.shipment_data.resource_caps_of_assigned_route.astype(str)
        self.subnet.shipment_data.route_cost_map = self.subnet.shipment_data.route_cost_map.astype(str)
        self.subnet.shipment_data.to_parquet(os.path.join(self.OUTPUT_DIR_FINAL, 'assignments.parquet'), index = False)


    def save_resource_file(self):
        if self.mode == 'DPS':
            self.subnet.resource_data[['index','origin','destination','type','cpt','cap','remaining_cap', 'lst_shadow_price', 'lst_assigned_time_stamp']].to_csv(os.path.join(self.OUTPUT_DIR_FINAL, 'resource_final_state.csv'), index = False)
            self.subnet.resource_data[['index','origin','destination','type','cpt','cap','remaining_cap', 'lst_shadow_price', 'lst_assigned_time_stamp']].to_parquet(os.path.join(self.OUTPUT_DIR_FINAL, 'resource_final_state.parquet'), index = False)
            
        # for hindsight info
        elif self.mode == 'no_cost':
            self.subnet.resource_data[['index','origin','destination','type','cpt','cap','remaining_cap', 'lst_assigned_time_stamp']].to_csv(os.path.join(self.OUTPUT_DIR_FINAL, 'resource_final_state.csv'), index = False)
            self.subnet.resource_data[['index','origin','destination','type','cpt','cap','remaining_cap', 'lst_assigned_time_stamp']].to_parquet(os.path.join(self.OUTPUT_DIR_FINAL, 'resource_final_state.parquet'), index = False)
            
        else:
            self.subnet.resource_data[['index','origin','destination','type','cpt','cap','remaining_cap', 'lst_shadow_price','hindsight_flow','hindsight_remaining_cap']].to_csv(os.path.join(self.OUTPUT_DIR_FINAL, 'resource_final_state.csv'), index = False)
            self.subnet.resource_data[['index','origin','destination','type','cpt','remaining_cap', 'lst_shadow_price', 'hindsight_flow','hindsight_remaining_cap']].to_parquet(os.path.join(self.OUTPUT_DIR_FINAL, 'resource_final_state.parquet'), index = False)
            
        
    def save_commodity_file(self):
        self.subnet.shipment_data.feasible_routes = self.subnet.shipment_data.feasible_routes.apply(lambda x: tuple(x))
        temp = self.subnet.shipment_data.groupby(['fc','ds','dimension','promise','feasible_routes','assigned_route']).size().reset_index(name='counts')
        temp = temp.groupby(['fc','ds','dimension','promise','feasible_routes']).apply(lambda x: list(zip(x.assigned_route, x.counts))).reset_index(name = 'cumulative_route_assignment')
        self.subnet.commodity_data.feasible_routes = self.subnet.commodity_data.feasible_routes.apply(lambda x: tuple(x))
        self.subnet.commodity_data = self.subnet.commodity_data.merge(temp, on = ['fc','ds','dimension','promise','feasible_routes'], how = 'left')
        if self.mode == 'DPS' or self.mode == 'no_cost':
            commodity_data_columns = ['fc','ds','dimension','promise','arrival','3p_variable_cost','ddu_variable_cost','AMZL_variable_cost','feasible_routes','units_from_shipment_data', 'cumulative_route_assignment']
        else:
            commodity_data_columns = ['fc','ds','dimension','promise','arrival','3p_variable_cost','ddu_variable_cost','amzl_variable_cost','feasible_routes','units_from_shipment_data', 'amzl_indirect_hindsight_flow', 'amzl_direct_hindsight_flow', 'ddu_hindsight_flow','3p_hindsight_flow', 'infeasible_hindsight_flow','hindsight_route_assignment', 'cumulative_route_assignment']
        self.subnet.commodity_data[commodity_data_columns].to_csv(os.path.join(self.OUTPUT_DIR_FINAL, 'commodity_final_state.csv'), index = False)
        

    def save_summary_file(self):
        summary_file = open(os.path.join(self.OUTPUT_DIR_FINAL, 'summary.csv'), "w")
        writer = csv.writer(summary_file)

        for key, value in self.subnet.summary_data.items():
            writer.writerow([key, value])
        summary_file.close()