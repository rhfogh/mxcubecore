import abc

class AbstractDataAnalysis(object):
    __metaclass__ = abc.ABCMeta


    @abc.abstractmethod
    def characterise(self, edna_input):
        """
        :returns: The EDNA Characterisation result XML.
        :str:
        """
        return None


    @abc.abstractmethod
    def get_html_report(self, edna_output):
        """
        :returns: The path to the html result report generated by EDNA.
        :rtype: str
        """
        return None


    @abc.abstractmethod
    def from_params(self, ref_parameters, char_params, path_str):
        """Return xml input file for EDNA

        :param ref_parameters: A named tuple or object with following fields:
              'id', 
              'prefix',
              'run_number',
              'template',
              'first_image',
              'num_images',
              'osc_start',
              'osc_range',
              'overlap',
              'exp_time',
              'num_passes',
              'comments',
              'path',
              'centred_positions',
              'energy',
              'resolution',
              'transmission',
              'shutterless',
              'inverse_beam',
              'screening_id'

         :param char_params: A named tuple or object with following fields:  
              # Optimisation parameters
              'aimed_resolution'
              'aimed_multiplicity'
              'aimed_i_sigma'
              'aimed_completness'
              'strategy_complexity'
              'induce_burn'
              'use_permitted_rotation'
              'permitted_phi_start'
              'permitted_phi_end'

              # Crystal
              'max_crystal_vdim'
              'min_crystal_vdim'
              'max_crystal_vphi'
              'min_crystal_vphi'
              'space_group'

              # Characterisation type
              'use_min_dose'
              'use_min_time'
              'min_dose'
              'min_time'
              'account_rad_damage'
              'not_use_low_res'
              'auto_res'
              'opt_sad'
              'determine_rad_params'

              # Radiation damage model
              'rad_suscept'
              'beta'
              'sigma'  

          :param path_str: Template string representing path to each image
        """
        pass


"""
The resulting EDNA XML can be handled with a function similair to this.
It has to be adapted to the specific representation of a collection that
you have.
"""
def dc_from_edna_output(edna_output, sample, dcg,  char_params = None):
    data_collections = []

    edna_result = XSDataResultMXCuBE.parseString(edna_output)

    try:
        char_results = edna_result.getCharacterisationResult()
        edna_strategy = char_results.getStrategyResult()
        collection_plan = edna_strategy.getCollectionPlan()[0]
        wedges = collection_plan.getCollectionStrategy().getSubWedge()
    except:
            pass
    else:
        try:
            run_number = collection_plan.getCollectionPlanNumber().getValue()
        except AttributeError:
            run_number = None

        try:
            resolution = collection_plan.getStrategySummary().\
                getResolution().getValue()
        except AttributeError:
            resolution = None

        try: 
            transmission = collection_plan.getStrategySummary().\
               getAttenuation().getValue()
        except AttributeError:
            transmission = None

        try:
            screening_id = edna_result.getScreeningId().getValue()
        except AttributeError:
            screening_id = None

        for wedge in wedges:
            exp_condition = wedge.getExperimentalCondition()
            goniostat = exp_condition.getGoniostat()
            beam = exp_condition.getBeam()

            dc = DataCollection()
            data_collections.append(dc)

            dc.parameters.prefix = QueueModelFactory.get_context().get_prefix(dc.parameters)

            if run_number:
                dc.parameters.run_number = run_number

            if resolution:
                dc.parameters.resolution = resolution

            if transmission:
                dc.parameters.transmission = transmission

            if screening_id:
                dc.parameters.screening_id = screening_id

            try:
                dc.parameters.osc_start = goniostat.\
                    getRotationAxisStart().getValue()
            except AttributeError:
                pass

            try:
                dc.parameters.osc_end = goniostat.\
                    getRotationAxisEnd().getValue()
            except AttributeError:
                pass

            try:
                dc.parameters.osc_width = goniostat.\
                    getOscillationWidth().getValue()
            except AttributeError:
                pass

            try:
                dc.parameters.num_images = \
                    int(abs(dc.parameters.osc_end - \
                            dc.parameters.osc_start) / dc.parameters.range)
            except AttributeError:
                pass

            try:
                dc.parameters.transmission = beam.getTransmission().getValue()
            except AttributeError:
                pass

            try: 
                dc.parameters.energy = \
                    int(123984.0/beam.getWavelength().getValue())/10000.0
            except AttributeError:
                pass

            try:
                dc.parameters.exp_time = beam.getExposureTime().getValue()
            except AttributeError:
                pass


            # dc.parameters.comments = enda_result.comments
            # dc.parametets.path = enda_result.directory
            # dc.parameters.centred_positions = enda_result.centred_positions
            # dc.parameters.energy = enda_result.energy
            # dc.parameters.esolution = enda_result.resolution
            # dc.parameters.transmission = enda_result.transmission
            # dc.parameters.screening_id = edna_resul.screening_id

            dc.sample = sample

            dc.parameters.directory = QueueModelFactory.collect_context.\
                get_image_directory(sub_dir = dcg.name.lower().replace(' ',''))

        if char_params:
            dc.char_params = char_params

    return data_collections
