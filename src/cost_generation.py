
import os
import numpy as np
import pandas as pd
import cvxpy as cp


class subnet_initialization():

    def __init__(self, path, CV, z, shipment_file_name, commodity_file_name, resource_file_name, LP_buffer_parameter):
        self.dataloader(path, commodity_file_name, resource_file_name)
        self.load_shipment_data(path, shipment_file_name)
        self.dic_order = self.shipment_data.to_dict(orient = 'index')
        self.dic_route = self.route_data.to_dict(orient = 'index')
        self.dic_commodity = self.commodity_data.to_dict(orient = 'index')
        self.dic_resource = self.resource_data.to_dict(orient = 'index')
        self.CV = CV
        self.z = z
        self.LP_buffer_parameter = LP_buffer_parameter 
        self.summary_data = {}
        self.create_sets_from_data()
        self.create_order_sets()
        self.create_variable_cost()
        

    def dataloader(self, path, commodity_file_name, resource_file_name):
        self.commodity_data =  pd.read_parquet(os.path.join(path,commodity_file_name +'.parquet'))
        self.resource_data =  pd.read_parquet(os.path.join(path,resource_file_name +'.parquet'))
        self.route_data = pd.read_parquet(os.path.join(path,'routes.parquet'))
        
    class commodity():
        def __init__(self,k, dic_commodity):
            inflate_3p = 1
            self.fc = dic_commodity[k]['fc']
            self.ds = dic_commodity[k]['ds']
            self.arrival = dic_commodity[k]['arrival']
            self.promise = dic_commodity[k]['promise']
            self.dimension = dic_commodity[k]['dimension']
            self.tp_variable_cost = dic_commodity[k]['3p_variable_cost']*inflate_3p
            self.amzl_variable_cost = dic_commodity[k]['AMZL_variable_cost']
            self.ddu_variable_cost = dic_commodity[k]['ddu_variable_cost']
            self.feasible_routes = dic_commodity[k]['feasible_routes']
            self.initial_units = dic_commodity[k]['units']
            self.units = dic_commodity[k]['units']
            self.units_from_shipment_data = dic_commodity[k]['units_from_shipment_data']

    class resource():
        def __init__(self,j, dic_resource):
            self.origin = dic_resource[j]['origin']
            self.destination = dic_resource[j]['destination']
            self.cpt = dic_resource[j]['cpt']
            self.cap = dic_resource[j]['cap']
            self.remaining_cap = dic_resource[j]['cap'] # set it to cap initially, will be modified every iteration
            self.optimal_flow = dic_resource[j]['cap'] # set it to cap initially, will be modified every iteration
            self.index = dic_resource[j]['index']
            self.type = dic_resource[j]['type']
            self.hard_or_soft = dic_resource[j]['hard_or_soft']
            self.incremental_cost = dic_resource[j]['incremental_cost']
            self.shadow_price = 'not yet calculated'
            self.penalty_cost = 'not yet calculated'
            self.target = 'not yet calculated'
            self.assignment_by_resolve = {}
            self.lst_assigned_time_stamp = []
            self.lst_shadow_price = []
            self.lst_remainingcap = []
            self.hindsight_flow = 'not yet calculated'

         


    class route():
        def __init__(self, r, dic_route):
            inflate_3p = 1
            self.resources = dic_route[r]['resources'] # put eval function here
            self.index = dic_route[r]['index']
            self.ds = dic_route[r]['ds']
            self.type = dic_route[r]['type']
            self.first_cpt = dic_route[r]['first_cpt']
            # assuming that base_cost only depends on route (no dimension group in commodity)
            if self.type == '3p':
                self.base_cost = dic_route[r]['base_cost']*inflate_3p
            else:
                self.base_cost = dic_route[r]['base_cost']
            self.route_cost = 'not yet calculated'

    class order():
        def __init__(self,o, dic_order):
            self.feasible_routes = dic_order[o]['feasible_routes']
            self.arrival = dic_order[o]['arrival']
            self.route_table = np.nan
            self.route_cost_map = np.nan
            self.assigned_route = 'not_yet_assigned'
            self.resource_caps_of_assigned_route = 'not_yet_assigned'
            self.hindsight_assigned_route = 'not_yet_assigned'

    def create_order_sets(self):
        self.O = [self.order(o, self.dic_order) for o in range(len(self.shipment_data))]


    def create_sets_from_data(self):
        self.K = [self.commodity(k, self.dic_commodity) for k in range(len(self.commodity_data))] # create commodity set
        self.J = [self.resource(j, self.dic_resource) for j in range(len(self.resource_data))] # create resource set
        self.R = [self.route(r, self.dic_route) for r in range(len(self.route_data))]
   
        def name_to_index(class_list):
            dic_name_to_index = {}
            for i in range(len(class_list)):
                dic_name_to_index[class_list[i].index] = i 
            return dic_name_to_index

        self.R_nametoindex = name_to_index(self.R)
        self.J_nametoindex = name_to_index(self.J)
        
        # create R_j
        self.R_j = {j: [] for j in range(len(self.J))}
        for r in range(len(self.R)):
            for resource in self.R[r].resources:
                j = self.J_nametoindex[resource]
                self.R_j[j].append(r)

        # create R_k, K_r
        self.K_r = {r: [] for r in range(len(self.R))}
        self.R_k = {k: [] for k in range(len(self.K))}
        for k in range(len(self.K)):
            feasible_routes = self.K[k].feasible_routes
            for route in feasible_routes:
                r = self.R_nametoindex[route]
                self.R_k[k].append(r)
                self.K_r[r].append(k)
    
    def create_variable_cost(self):
        # note that this is not generalized in the sense that we assume the route costs are independent of commodities (ok for the 2-SC network)
        self.c_kr = {}
        for k in range(len(self.K)):
            for r in self.R_k[k]:
                self.c_kr[(k,r)] = self.R[r].base_cost

                
    def load_shipment_data(self, path, shipment_file_name):
        self.shipment_data = pd.read_parquet(os.path.join(path, shipment_file_name +'.parquet')).sort_values(by = 'arrival')
        self.shipment_data.arrival = pd.to_datetime(self.shipment_data.arrival)
        self.shipment_data = self.shipment_data.reset_index(drop = True)
        self.exp_start = self.shipment_data.arrival.min()
        self.exp_end = self.shipment_data.arrival.max()
        self.exp_time_hoirzon = self.exp_end - self.exp_start

class formulate_and_solve_QP:
    def __init__(self, subnet, penalize_variable_cost):
        self.subnet = subnet
        self.penalize_variable_cost = penalize_variable_cost
        self.build_and_solve_QP()
        
    def add_variables(self):
        self.y_kr = {}
        self.g_j = {}
        self.g_k = {}
        for k in range(len(self.subnet.K)):
            for r in self.subnet.R_k[k]:
                self.y_kr[(k,r)] = cp.Variable(integer=False, nonneg=True)        
        for j in range(len(self.subnet.J)):
            self.g_j[j] = cp.Variable(integer=False, nonneg=True)
        for k in range(len(self.subnet.K)):
            self.g_k[k] = cp.Variable(integer=False, nonneg=True)   

    def add_soft_cap_constraints(self):
        for j in range(len(self.subnet.J)):
            self.constraints.append(sum(self.y_kr[(k,r)] for r in self.subnet.R_j[j] for k in self.subnet.K_r[r]) - self.subnet.J[j].target <= self.g_j[j])
    
    def add_demand_constraints(self):
        for k in range(len(self.subnet.K)):
            self.constraints.append(sum(self.y_kr[(k,r)] for r in self.subnet.R_k[k]) + self.g_k[k]== self.subnet.K[k].units)

    def remove_resources_with_zero_remaining_cap(self):
        for j in range(len(self.subnet.J)):
            if self.subnet.J[j].remaining_cap == 0:
                self.constraints.append(self.g_j[j] == 0)
    
    def mute_3p_route(self):
        for r in range(len(self.subnet.R)):
            if self.subnet.R[r].type == '3p':
                for k in self.subnet.K_r[r]:
                    self.constraints.append(self.y_kr[(k,r)]==0)

    def add_objective(self):
        quadratic_cost = 0.5*sum(self.subnet.J[j].penalty_cost*(self.g_j[j]**2) for j in range(len(self.subnet.J)))
        if self.penalize_variable_cost == True:
            variable_cost = sum(self.y_kr[(k,r)]*self.subnet.c_kr[(k,r)] for k in range(len(self.subnet.K))for r in self.subnet.R_k[k])
        else:
            variable_cost = 0
        C = 1000000*np.max([self.subnet.c_kr[(k,r)] for k in range(len(self.subnet.K))for r in self.subnet.R_k[k]])
        infeasible_cost = C*sum(self.g_k[k] for k in range(len(self.subnet.K)))
        self.objective  =cp.Minimize(quadratic_cost + variable_cost + infeasible_cost)

    def get_solutions(self): # get primal and dual solutions
        # primal solution
        self.subnet.y_kr_sol = {}
        for k in range(len(self.subnet.K)):
            for r in self.subnet.R_k[k]:
                self.subnet.y_kr_sol[(k,r)] = self.y_kr[(k,r)].value

        self.subnet.g_j_sol = []
        # self.total_flow_j_sol= []
        for j in range(len(self.subnet.J)):
            self.subnet.g_j_sol.append(self.g_j[j].value)
            self.subnet.J[j].optimal_flow = sum(self.subnet.y_kr_sol[(k,r)] for r in self.subnet.R_j[j] for k in self.subnet.K_r[r])
        self.subnet.resource_duals = [self.constraints[i].dual_value for i in range(0, len(self.subnet.J))]

    def build_and_solve_QP(self):
        self.constraints = []
        self.add_variables()
        self.add_soft_cap_constraints()
        self.add_demand_constraints()
        self.remove_resources_with_zero_remaining_cap()
        # if self.mode == 'DPS_QPno3P':
        #     self.mute_3p_route()
        # self.constraints = [value for value in self.constraints if value != True]
        self.add_objective()
        QP = cp.Problem(self.objective, self.constraints)
        obj_value = QP.solve(solver=cp.GUROBI)
        self.get_solutions()


class pre_process():
    def __init__(self, subnet, iter, i, QP_or_LP):
        self.subnet = subnet
        self.iter = iter
        self.i = i
        self.QP_or_LP = QP_or_LP
        if self.QP_or_LP == 'QP':
            self.update_demand_forecast()
            self.update_standard_deviation()
            self.update_target()
            self.update_penalty_cost()
        elif self.QP_or_LP == 'LP':
            self.update_demand_forecast()
        else:
            print('error in pre process')

    def update_demand_forecast(self):

        def find_the_next_arrival(k):
            """ given a commodity and a arrival time, 
            produce the next arrival time of the same commodity"""

            list_of_same_commodity_indexes = [kk for kk in range(len(self.subnet.K))
             if (self.subnet.K[kk].ds == self.subnet.K[k].ds)
             &(self.subnet.K[kk].fc == self.subnet.K[k].fc)
             &(self.subnet.K[kk].dimension == self.subnet.K[k].dimension)
             &(self.subnet.K[kk].promise == self.subnet.K[k].promise)]

            list_of_arrival = [self.subnet.K[kk].arrival for kk in list_of_same_commodity_indexes
            if self.subnet.K[kk].arrival > self.subnet.K[k].arrival]
            
            if list_of_arrival != []:
                return min(list_of_arrival)
            else:
                return self.subnet.exp_end

        for k in range(len(self.subnet.K)):
            
            resolve_time = self.subnet.exp_start + (self.i/self.iter)*self.subnet.exp_time_hoirzon
            arrival = self.subnet.K[k].arrival
            next_arrival = find_the_next_arrival(k)

            if resolve_time <= arrival:
                self.subnet.K[k].units = self.subnet.K[k].initial_units
            elif (resolve_time > arrival) and (resolve_time <= next_arrival):
                linear_discount = (next_arrival - resolve_time)/(next_arrival - arrival)
                self.subnet.K[k].units = self.subnet.K[k].initial_units*linear_discount
            elif resolve_time > next_arrival:
                self.subnet.K[k].units = 0
            else:
                print('error in updating demand forecast')

    def update_standard_deviation(self):
        for j in range(len(self.subnet.J)):
            self.subnet.J[j].sigma = self.subnet.CV*self.subnet.J[j].remaining_cap

    def update_target(self):
        for j in range(len(self.subnet.J)):
            self.subnet.J[j].target = self.subnet.J[j].remaining_cap - self.subnet.z*self.subnet.J[j].sigma

    def update_penalty_cost(self):
        for j in range(len(self.subnet.J)):
            if self.subnet.J[j].sigma != 0:
                self.subnet.J[j].penalty_cost = 0.798*self.subnet.J[j].incremental_cost/((self.subnet.J[j].sigma)*self.subnet.z**2)
            else:
                self.subnet.J[j].penalty_cost = 100000000
                
class save_intermediate_states():
    def __init__(self, subnet, which_iter, path, QP_or_LP):
        self.digit = 2
        self.subnet = subnet
        self.path = path
        self.which_iter = which_iter
        self.QP_or_LP = QP_or_LP
        self.update_resource_data()
        self.update_route_data()
        self.update_commodity_data()
        self.save_commodity_data()
        self.save_route_data()
        self.save_resource_data()

    def update_resource_data(self):

        if self.QP_or_LP == 'QP':

            self.subnet.resource_data['target'] = np.array([self.subnet.J[j].target for j in range(len(self.subnet.J))])
            self.subnet.resource_data['sigma'] = np.array([self.subnet.J[j].sigma for j in range(len(self.subnet.J))])
            self.subnet.resource_data['penalty_cost_coefficient'] = np.array([self.subnet.J[j].penalty_cost for j in range(len(self.subnet.J))])
            self.subnet.resource_data['dual'] = np.array(self.subnet.resource_duals)
            self.subnet.resource_data['excess_flow'] = np.array(self.subnet.g_j_sol)
            self.subnet.resource_data['optimal_flow'] = np.array([self.subnet.J[j].optimal_flow for j in range(len(self.subnet.J))])
            self.subnet.resource_data['remaining_cap'] = np.array([self.subnet.J[j].remaining_cap for j in range(len(self.subnet.J))])
            
            for j in range(len(self.subnet.J)):
                self.subnet.J[j].shadow_price = np.array(self.subnet.resource_duals)[j]
                self.subnet.J[j].lst_shadow_price.append(self.subnet.J[j].shadow_price)
                self.subnet.J[j].excess_flow = np.array(self.subnet.g_j_sol)[j]
        
        elif self.QP_or_LP == 'LP':

            self.subnet.resource_data['penalty_cost_coefficient'] = np.array([self.subnet.J[j].penalty_cost for j in range(len(self.subnet.J))])
            self.subnet.resource_data['dual'] = np.array(self.subnet.resource_duals)
            self.subnet.resource_data['optimal_flow'] = np.array([self.subnet.J[j].optimal_flow for j in range(len(self.subnet.J))])
            self.subnet.resource_data['remaining_cap'] = np.array([self.subnet.J[j].remaining_cap for j in range(len(self.subnet.J))])
            
            for j in range(len(self.subnet.J)):
                self.subnet.J[j].shadow_price = np.array(self.subnet.resource_duals)[j]
                self.subnet.J[j].lst_shadow_price.append(self.subnet.J[j].shadow_price)
                
        else:
            print('error in update resource data')

    def update_route_data(self):
        self.subnet.route_data['optimal_flow'] = 0
        for r in range(len(self.subnet.R)):
            route_index = self.subnet.R[r].index
            flow = sum(self.subnet.y_kr_sol[(k,r)] for k in self.subnet.K_r[r])
            self.subnet.route_data.loc[self.subnet.route_data['index'] == route_index, 'optimal_flow'] = flow

    def update_commodity_data(self):
        # commodity data
        flows_on_routes_all = []
        self.subnet.commodity_data['amzl_optimal_flow'] = 0
        self.subnet.commodity_data['ddu_optimal_flow'] = 0
        self.subnet.commodity_data['3p_optimal_flow'] = 0
        for k in range(len(self.subnet.K)):
            for r in self.subnet.R_k[k]:
                if self.subnet.R[r].type == '3p':
                    self.subnet.commodity_data.loc[k,'3p_optimal_flow'] += self.subnet.y_kr_sol[(k,r)]
                elif self.subnet.R[r].type == 'ddu':
                    self.subnet.commodity_data.loc[k,'ddu_optimal_flow'] += self.subnet.y_kr_sol[(k,r)]
                else:
                    self.subnet.commodity_data.loc[k,'amzl_optimal_flow'] += self.subnet.y_kr_sol[(k,r)]

            flows_on_routes = {}
            for r in self.subnet.R_k[k]:
                flows_on_routes[self.subnet.R[r].index] = self.subnet.y_kr_sol[(k,r)]
            flows_on_routes_all.append(flows_on_routes)

        self.subnet.commodity_data['optimal_flows_on_each_route'] = np.array(flows_on_routes_all)
        self.subnet.commodity_data['units'] = np.array([self.subnet.K[k].units for k in range(len(self.subnet.K))])

    def save_commodity_data(self):
        self.subnet.commodity_data.drop(columns = ['units_from_shipment_data']).to_csv(os.path.join(self.path,'commodity_intermediate_'+ str(self.which_iter)+'.csv'), index = False)
        
    def save_route_data(self):
        self.subnet.route_data.to_csv(os.path.join(self.path,'route_intermediate_'+ str(self.which_iter)+'.csv'), index = False)
        
    def save_resource_data(self):
        self.subnet.resource_data.to_csv(os.path.join(self.path,'resource_intermediate_'+ str(self.which_iter)+'.csv'), index = False)
        

class formulate_and_solve_LP:
    def __init__(self, subnet):
        self.subnet = subnet
        self.build_and_solve_LP()
        
    def add_variables(self):
        self.y_kr = {}
        self.g_k = {}
        for k in range(len(self.subnet.K)):
            for r in self.subnet.R_k[k]:
                self.y_kr[(k,r)] = cp.Variable(integer=False, nonneg=True)           
        for k in range(len(self.subnet.K)):
            self.g_k[k] = cp.Variable(integer=False, nonneg=True)

    def add_hard_cap_constraints(self):
        for j in range(len(self.subnet.J)):
            self.constraints.append(sum(self.y_kr[(k,r)] for r in self.subnet.R_j[j] for k in self.subnet.K_r[r]) <= self.subnet.LP_buffer_parameter*self.subnet.J[j].remaining_cap)
    
    def add_demand_constraints_with_slack(self):
        for k in range(len(self.subnet.K)):
            self.constraints.append(sum(self.y_kr[(k,r)] for r in self.subnet.R_k[k]) + self.g_k[k] == self.subnet.K[k].units)
    

    def add_objective(self):
        variable_cost = sum(self.y_kr[(k,r)]*self.subnet.c_kr[(k,r)] for k in range(len(self.subnet.K))for r in self.subnet.R_k[k])
        C =10000000*np.max([self.subnet.c_kr[(k,r)] for k in range(len(self.subnet.K))for r in self.subnet.R_k[k]])
        infeasible_cost = C*sum(self.g_k[k] for k in range(len(self.subnet.K)))
        self.objective  = cp.Minimize(variable_cost + infeasible_cost)

    def get_solutions(self): # get primal and dual solutions

        # primal solution
        self.subnet.y_kr_sol = {}
        for k in range(len(self.subnet.K)):
            for r in self.subnet.R_k[k]:
                self.subnet.y_kr_sol[(k,r)] = self.y_kr[(k,r)].value

        self.subnet.g_k_sol = []
        for k in range(len(self.subnet.K)):
            self.subnet.g_k_sol.append(self.g_k[k].value)

        for j in range(len(self.subnet.J)):
            self.subnet.J[j].optimal_flow = sum(self.subnet.y_kr_sol[(k,r)] for r in self.subnet.R_j[j] for k in self.subnet.K_r[r])

        self.subnet.resource_duals = [self.constraints[i].dual_value for i in range(0, len(self.subnet.J))]

    def build_and_solve_LP(self):
        self.constraints = []
        self.add_variables()
        self.add_hard_cap_constraints()
        self.add_demand_constraints_with_slack()
        self.add_objective()
        LP = cp.Problem(self.objective, self.constraints)
        LP.solve(solver=cp.GUROBI)
        self.get_solutions()
