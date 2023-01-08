#%%
from src import cost_generation, order_fulfillment, evaluation, util
import random 
import os
import time

#%%

def run(cfg):

    # random seed 
    random.seed(cfg['seed'])

    # make directory
    util.make_dir(cfg['OUTPUT_DIR'])

    # run the QP algorithm 
    if cfg['DPS'] == True:

        # make directory 
        util.make_dir(os.path.join(cfg['OUTPUT_DIR'], 'QP', 'intermediate_state'))
        util.make_dir(os.path.join(cfg['OUTPUT_DIR'], 'QP', 'final_state'))

        # DPS initialization
        t_start = time.time()
        subnet = cost_generation.subnet_initialization(
            cfg['INPUT_DIR'], 
            cfg['cv'], 
            cfg['z'], 
            cfg['shipment_file_name'],
            cfg['commodity_file_name'],
            cfg['resource_file_name'],
            cfg['LP_buffer_parameter'])
        t_end = time.time()
        print(f'DPS initialization: {((t_end - t_start) / 60):.3f} min(s)')  
        
        # run DPS assignment
        for i in range(cfg['iter']):
            t_start = time.time()

            # update demand forecast, standard deviation, target, penalty coefficient
            cost_generation.pre_process(
                subnet, 
                cfg['iter'], 
                i,
                'QP') 

            # generate primal, dual solutions, sotre information to subnet dataframe
            cost_generation.formulate_and_solve_QP(subnet, cfg['penalize_variable_cost'])
            cost_generation.save_intermediate_states(
                subnet,
                i,
                os.path.join(cfg['OUTPUT_DIR'], 'QP' ,'intermediate_state'),
                'QP')

            # make assignment decisions, update states 
            order_fulfillment.assign(
                'DPS', 
                subnet, 
                cfg['iter'],
                i,
                cfg['intermediate_update_shadow_price'],
                cfg['tie_break_rule'],
                cfg['decimal']) 
            t_end = time.time()


        evaluation.all(subnet)
        evaluation.save_final_states(subnet, 'DPS', os.path.join(cfg['OUTPUT_DIR'], 'QP', 'final_state'))

        util.save_to_exel(os.path.join(cfg['OUTPUT_DIR'], 'QP', 'final_state'), 'final_state')
        util.save_to_exel(os.path.join(cfg['OUTPUT_DIR'], 'QP', 'intermediate_state'), 'intermediate_state')
    # 

    # run the LP algorithm 
    if cfg['LP_solution'] == True:

        # make directory
        util.make_dir(os.path.join(cfg['OUTPUT_DIR'], 'LP', 'intermediate_state'))
        util.make_dir(os.path.join(cfg['OUTPUT_DIR'], 'LP', 'final_state'))


        # initialization
        subnet = cost_generation.subnet_initialization(
            cfg['INPUT_DIR'], 
            cfg['cv'], 
            cfg['z'], 
            cfg['shipment_file_name'],
            cfg['commodity_file_name'],
            cfg['resource_file_name'],
            cfg['LP_buffer_parameter'])
        
        for i in range(cfg['iter']):

            # update demand forecast, standard deviation, target, penalty coefficient
            cost_generation.pre_process(
                subnet, 
                cfg['iter'], 
                i,
                'LP') 

            # generate primal, dual solutions, sotre information to subnet dataframe
            cost_generation.formulate_and_solve_LP(subnet)
            cost_generation.save_intermediate_states(
                subnet,
                i,
                os.path.join(cfg['OUTPUT_DIR'], 'LP', 'intermediate_state'),
                'LP')

            # make assignment decisions, update states 
            order_fulfillment.assign(
                'DPS', 
                subnet, 
                cfg['iter'],
                i,
                cfg['intermediate_update_shadow_price'],
                cfg['tie_break_rule'],
                cfg['decimal']) 

        
        evaluation.all(subnet)
        evaluation.save_final_states(subnet, 'DPS', os.path.join(cfg['OUTPUT_DIR'], 'LP', 'final_state'))

        util.save_to_exel(os.path.join(cfg['OUTPUT_DIR'], 'LP', 'final_state'), 'final_state')
        util.save_to_exel(os.path.join(cfg['OUTPUT_DIR'], 'LP', 'intermediate_state'), 'intermediate_state')
    

    # Run benchmark solutions
    if cfg['benchmark_solution'] == True: 
        
        util.make_dir(os.path.join(cfg['OUTPUT_DIR'], 'benchmark_solution'))
        
        # initialization
        subnet = cost_generation.subnet_initialization(
        cfg['INPUT_DIR'], 
        cfg['cv'], 
        cfg['z'], 
        cfg['shipment_file_name'],
        cfg['commodity_file_name'],
        cfg['resource_file_name'],
        cfg['LP_buffer_parameter'])

        # greedy
        order_fulfillment.assign(
            'greedy',
            subnet,
            1,
            0,
            cfg['intermediate_update_shadow_price'],
            cfg['tie_break_rule'],
            cfg['decimal']) # make assignment decisions, update states 

        # hindsight solution
        evaluation.all(subnet)
        evaluation.Hindsight_solution(subnet)
        evaluation.save_final_states(subnet, 'greedy', os.path.join(cfg['OUTPUT_DIR'], 'benchmark_solution'))

        util.save_to_exel(os.path.join(cfg['OUTPUT_DIR'], 'benchmark_solution'), 'benchmark_solution')

    # Make route choice randomly as if all routes are free 
    if cfg['no_cost'] == True: 
        
        util.make_dir(os.path.join(cfg['OUTPUT_DIR'], 'no_cost'))

        # initialization
        subnet = cost_generation.subnet_initialization(
        cfg['INPUT_DIR'], 
        cfg['cv'], 
        cfg['z'], 
        cfg['shipment_file_name'],
        cfg['commodity_file_name'],
        cfg['resource_file_name'],
        cfg['LP_buffer_parameter'])

        # greedy
        order_fulfillment.assign(
            'no_cost',
            subnet,
            1,
            0,
            cfg['intermediate_update_shadow_price'],
            cfg['tie_break_rule'],
            cfg['decimal']) # make assignment decisions, update states 

        # hindsight solution
        evaluation.all(subnet)
        evaluation.save_final_states(subnet, 'no_cost', os.path.join(cfg['OUTPUT_DIR'], 'no_cost'))



        util.save_to_exel(os.path.join(cfg['OUTPUT_DIR'], 'no_cost'), 'no_cost')

    if cfg['DPS_QPno3P'] == True:
        # make directory 
        util.make_dir(os.path.join(cfg['OUTPUT_DIR'], 'DPS_QPno3P', 'intermediate_state'))
        util.make_dir(os.path.join(cfg['OUTPUT_DIR'], 'DPS_QPno3P', 'final_state'))


        # DPS initialization
        subnet = cost_generation.subnet_initialization(
            cfg['INPUT_DIR'], 
            cfg['cv'], 
            cfg['z'], 
            cfg['shipment_file_name'],
            cfg['commodity_file_name'],
            cfg['resource_file_name'],
            cfg['LP_buffer_parameter'])
        
        # run DPS assignment

        for i in range(cfg['iter']):

            # update demand forecast, standard deviation, target, penalty coefficient
            cost_generation.pre_process(
                subnet, 
                cfg['iter'], 
                i,
                'QP') 

            # generate primal, dual solutions, sotre information to subnet dataframe
            cost_generation.formulate_and_solve_QP(subnet, cfg['penalize_variable_cost'])
            cost_generation.save_intermediate_states(
                subnet,
                i,
                os.path.join(cfg['OUTPUT_DIR'], 'DPS_QPno3P' ,'intermediate_state'),
                'DPS_QPno3P')

            # make assignment decisions, update states 
            order_fulfillment.assign(
                'DPS_QPno3P', 
                subnet, 
                cfg['iter'],
                i,
                cfg['intermediate_update_shadow_price'],
                cfg['tie_break_rule'],
                cfg['decimal']) 

        evaluation.all(subnet)
        evaluation.save_final_states(subnet, 'DPS', os.path.join(cfg['OUTPUT_DIR'], 'DPS_QPno3P', 'final_state'))

        util.save_to_exel(os.path.join(cfg['OUTPUT_DIR'], 'DPS_QPno3P', 'final_state'), 'final_state')
        util.save_to_exel(os.path.join(cfg['OUTPUT_DIR'], 'DPS_QPno3P', 'intermediate_state'), 'intermediate_state')
    
