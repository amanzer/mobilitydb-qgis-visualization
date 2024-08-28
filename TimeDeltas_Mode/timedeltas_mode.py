"""

TimeDeltas Mode Script


This script is designed to be executed within QGIS's Python console.
Before running, create the 'id' serial column and update the database connection parameters.



Author: Manzer Ali
Date: August 12, 2024

"""

from pymeos.db.psycopg import MobilityDB
from pymeos import *
import time
from shapely.geometry import Point
import pickle

from qgis.PyQt.QtCore import QVariant

from qgis.core import (
    Qgis,
    QgsVectorLayerTemporalProperties,
    QgsFeature,
    QgsGeometry,
    QgsVectorLayer, 
    QgsField, 
    QgsProject,
    QgsTemporalNavigationObject,
    QgsTaskManager,
    QgsTask,
    QgsMessageLog,
    QgsDateTimeRange,    
)
from PyQt5.QtCore import QDateTime


# DATABASE PARAMETERS

DATABASE_USER = ""
DATABASE_PASSWORD = ""
DATABASE_HOST = ""
DATABASE_PORT = ""
LIMIT = 50000
DATABASE_NAME = ""
TPOINT_TABLE_NAME = ""
TPOINT_COLUMN_NAME = ""


# CONSTANTS
TIME_DELTA_SIZE = 30
TPOINT_ID_COLUMN_NAME = "id" 



class WaitingTDelta:
    """
    A helper class to manage the waiting state for background QGIS Tasks.
    """

    def __init__(self) -> None:
        """
        Initializes the waiting state to False.
        """
        self.waiting: bool = False

    def set_waiting(self, status: bool) -> None:
        """
        Sets the waiting status.

        :param status: A boolean indicating if the system is in a waiting state.
        """
        self.waiting = status

    def is_waiting(self) -> bool:
        """
        Returns the current waiting status.

        :return: A boolean indicating the current waiting status.
        """
        return self.waiting

def log(msg: str) -> None:
    """
    Logs a message to the QGIS message log under the 'Move' tab with an info level.

    :param msg: The message to log.
    """
    QgsMessageLog.logMessage(msg, 'Move', level=Qgis.Info)

class DatabaseController:
    """
    Class to handle the MobilityDB connections and data retrieval.
    """

    def __init__(self, connection_parameters: dict) -> None:
        """
        Initializes the DatabaseController with connection parameters.

        :param connection_parameters: A dictionary containing database connection parameters.
        """
        self.connection_params = {
            "host": connection_parameters["host"],
            "port": connection_parameters["port"],
            "dbname": connection_parameters["dbname"],
            "user": connection_parameters["user"],
            "password": connection_parameters["password"],
        }
        self.table_name = connection_parameters["table_name"]
        self.id_column_name = connection_parameters["id_column_name"]
        self.tpoint_column_name = connection_parameters["tpoint_column_name"]


    def get_IDs_timestamps(self, limit: int) -> tuple:
        """
        Fetch the IDs and the start/end timestamps of the TgeomPoints.
        This data is used to create the QGIS feature for each tgeompoint.

        :param limit: The maximum number of records to retrieve.
        :return: A tuple containing a list of results and the SRID, or None if an error occurred.
        """
        query_srid = ""
        query = ""
        try:
            connection = MobilityDB.connect(**self.connection_params)
            cursor = connection.cursor()

            # Fetch SRID of the TgeomPoint column
            query_srid = f"SELECT srid({self.tpoint_column_name}) FROM public.{self.table_name} LIMIT 1;"
            cursor.execute(query_srid)
            srid = cursor.fetchall()[0][0]

            # Fetch IDs and timestamps
            query = (
                f"SELECT {self.id_column_name}, "
                f"startTimestamp({self.tpoint_column_name}), "
                f"endTimestamp({self.tpoint_column_name}) "
                f"FROM public.{self.table_name} LIMIT {limit};"
            )
            cursor.execute(query)
            
            results = []
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                results.extend(rows)

            cursor.close()
            connection.close()
            return results, srid

        except Exception as e:
            log(f"Error in fetching IDs and timestamps: {e}\nQuery SRID: {query_srid}\nQuery: {query}")
            return None, None

    def get_TgeomPoints(self, start_ts: str, end_ts: str, limit: int) -> list:
        """
        Fetch the TgeomPoints for the given time range.

        :param start_ts: The start timestamp of the time range.
        :param end_ts: The end timestamp of the time range.
        :param limit: The maximum number of records to retrieve.
        :return: A list of TgeomPoints within the specified time range, or None if an error occurred.
        """
        query = ""
        try:
            connection = MobilityDB.connect(**self.connection_params)
            cursor = connection.cursor()

            # Query TgeomPoints within the specified time range
            query = (
                f"SELECT attime(a.{self.tpoint_column_name}::tgeompoint,"
                f"span('{start_ts}'::timestamptz, '{end_ts}'::timestamptz, true, true)) "
                f"FROM public.{self.table_name} AS a LIMIT {limit};"
            )
            cursor.execute(query)
            
            results = []
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                results.extend(rows)

            cursor.close()
            connection.close()
            return results

        except Exception as e:
            log(f"Error in fetching TgeomPoints: {e}\nQuery: {query}")
            return None

    def get_min_timestamp(self) -> str:
        """
        Returns the minimum timestamp of the TgeomPoints in the table.

        :return: The earliest timestamp as a string, or None if an error occurred.
        """
        query = ""
        try:
            connection = MobilityDB.connect(**self.connection_params)
            cursor = connection.cursor()
            query = f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};"
            cursor.execute(query)
            res = cursor.fetchone()[0]

            cursor.close()
            connection.close()
            return res

        except Exception as e:
            log(f"Error in fetching minimum timestamp: {e}\nQuery: {query}")
            return None

    def get_max_timestamp(self) -> str:
        """
        Returns the maximum timestamp of the TgeomPoints in the table.

        :return: The latest timestamp as a string, or None if an error occurred.
        """
        query = ""
        try:
            connection = MobilityDB.connect(**self.connection_params)
            cursor = connection.cursor()
            query = f"SELECT MAX(endTimestamp({self.tpoint_column_name})) AS latest_timestamp FROM public.{self.table_name};"
            cursor.execute(query)
            res = cursor.fetchone()[0]

            cursor.close()
            connection.close()
            return res

        except Exception as e:
            log(f"Error in fetching maximum timestamp: {e}\nQuery: {query}")
            return None

    

class VectorLayerController:
    """
    Controller for an in-memory vector layer to view TgeomPoints.

    The layer is designed to store geometries of TgeomPoints at a given timestamp. 
    The temporal properties allow temporal navigation and visualization of the data.
    """

    def __init__(self, srid):
        """
        Initialize the VectorLayerController.

        Parameters:
        - srid (int): The spatial reference system identifier (SRID) used to define the coordinate reference system (CRS) of the vector layer.
        """
        # Create an in-memory vector layer with a point geometry type and the specified CRS.
        self.vlayer = QgsVectorLayer(f"Point?crs=epsg:{srid}", "MobilityBD Data", "memory")
        
        # Define the fields (attributes) for the vector layer.
        fields = [
            QgsField("id", QVariant.String),          # ID of the feature 
            QgsField("start_time", QVariant.DateTime), # Start timestamp of the temporal point
            QgsField("end_time", QVariant.DateTime)    # End timestamp of the temporal point
        ]
        
        # Add the fields to the layer's data provider and update the layer to include these fields.
        self.vlayer.dataProvider().addAttributes(fields)
        self.vlayer.updateFields()

        # Set up the temporal properties of the layer to enable temporal navigation.
        tp = self.vlayer.temporalProperties()
        tp.setIsActive(True)  # Enable temporal properties for the layer
        tp.setMode(QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
        tp.setStartField("start_time")  # Specify the field containing the start time
        tp.setEndField("end_time")      # Specify the field containing the end time

        # Ensure the layer's fields and temporal properties are updated.
        self.vlayer.updateFields()
        
        # Add the vector layer to the QGIS project.
        QgsProject.instance().addMapLayer(self.vlayer)

    def get_vlayer_fields(self):
        """
        Retrieve the fields of the vector layer.

        Returns:
        - QgsFields: The fields (attributes) of the vector layer, or None if the layer does not exist.
        """
        if self.vlayer:
            return self.vlayer.fields()
        return None

    def add_features(self, features_list):
        """
        Add features to the vector layer.

        Parameters:
        - features_list (list): A list of QgsFeature objects to be added to the vector layer.
        """
        if self.vlayer:
            self.vlayer.dataProvider().addFeatures(features_list)

    def __del__(self):
        """
        Destructor to clean up the vector layer.

        Removes the vector layer from the QGIS project upon deletion of the VectorLayerController instance.
        """
        if self.vlayer:
            QgsProject.instance().removeMapLayer(self.vlayer)
            self.vlayer = None




class InitializeFeaturesTask(QgsTask):
    """
    A QGIS task to initialize features in a QGIS project by fetching IDs and timestamps from a database.

    This class extends `QgsTask` to perform a task in the background, specifically designed to initialize QGIS features 
    by retrieving IDs and their associated start/end timestamps from a database using the provided database connector.

    Parameters:
    - description (str): A description of the task.
    - project_title (str): The title of the QGIS project.
    - database_connector (DatabaseController): An instance of the DatabaseController class to interact with the database.
    - finished_fnc (callable): A function to call upon successful completion of the task.
    - failed_fnc (callable): A function to call if the task fails.

    Attributes:
    - result_params (dict): Stores the result parameters after the task is completed, including fetched IDs and timestamps.
    - error_msg (str): Stores the error message if the task fails.
    """

    def __init__(self, description, project_title, database_connector, finished_fnc, failed_fnc):
        super(InitializeFeaturesTask, self).__init__(description, QgsTask.CanCancel)
        self.project_title = project_title
        self.database_connector = database_connector
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc
        self.result_params = None
        self.error_msg = None
    
    def finished(self, result):
        """
        Called when the task finishes.

        Parameters:
        - result (bool): Indicates whether the task was successful.
        """
        if result:
            self.finished_fnc(self.result_params)
        else:
            self.failed_fnc(self.error_msg)

    def run(self):
        """
        Executes the task to fetch IDs and timestamps from the database.

        This method runs in the background when the task is executed. It fetches IDs and their start/end timestamps from the database. 
        If successful, the data is stored in `result_params`, otherwise, an error message is logged.

        Returns:
        - bool: True if the task was successful, False otherwise.
        """
        try:
            results, srid = self.database_connector.get_IDs_timestamps(LIMIT)
            
            self.result_params = {
                'Ids_timestamps': results,
                'srid': srid
            }
        except Exception as e:
            log(f"Error in fetching IDs and timestamps : {e}")
            self.error_msg = str(e)
            return False
        return True

class FetchTimeDeltaTask(QgsTask):
    """
    A QGIS task to fetch TgeomPoints for a specified time delta from the database.

    This class extends `QgsTask` to perform a task in the background, specifically designed to fetch 
    TgeomPoints from the database for a given time range. The results are processed and passed to the 
    callback functions.

    Parameters:
    - description (str): A description of the task.
    - project_title (str): The title of the QGIS project.
    - database_connector (DatabaseController): An instance of the DatabaseController class to interact with the database.
    - begin_timestamp (str): The start of the time range to fetch TgeomPoints.
    - end_timestamp (str): The end of the time range to fetch TgeomPoints.
    - limit (int): The maximum number of records to fetch.
    - id (str): An identifier for the task, useful for distinguishing between different tasks.
    - finished_fnc (callable): A function to call upon successful completion of the task.
    - failed_fnc (callable): A function to call if the task fails.

    Attributes:
    - result_params (dict): Stores the result parameters after the task is completed, including the fetched TgeomPoints.
    - error_msg (str): Stores the error message if the task fails.
    """

    def __init__(self, description, project_title, database_connector, begin_timestamp, end_timestamp, limit, id, finished_fnc, failed_fnc):
        super(FetchTimeDeltaTask, self).__init__(description, QgsTask.CanCancel)
        self.project_title = project_title
        self.database_connector = database_connector
        self.begin_timestamp = begin_timestamp
        self.end_timestamp = end_timestamp
        self.limit = limit
        self.id = id
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc
        self.result_params = None
        self.error_msg = None
    
    def finished(self, result):
        """
        Called when the task finishes.

        Parameters:
        - result (bool): Indicates whether the task was successful.
        """
        if result:
            self.finished_fnc(self.result_params)
        else:
            self.failed_fnc(self.error_msg)

    def run(self):
        """
        Executes the task to fetch TgeomPoints for the specified time delta.

        This method runs in the background when the task is executed. It fetches TgeomPoints from the database 
        within the specified time range. If successful, the data is stored in `result_params`, otherwise, an error message is logged.

        Returns:
        - bool: True if the task was successful, False otherwise.
        """
        try:
            tgeompoints = self.database_connector.get_TgeomPoints(self.begin_timestamp, self.end_timestamp, self.limit)
            self.result_params = {
                'id': self.id,
                'TgeomPoints_list': tgeompoints
            }
        except Exception as e:
            log(f"Error in fetching time delta TgeomPoints : {e}")
            self.error_msg = str(e)
            return False
        return True




class MobilitydbLayerHandler:
    """
    Initializes and handles a vector layer through an instance of VectorLayerController.
    
    """

    def __init__(self, iface, task_manager, database_controller, limit, WaitingTDelta, start_ts, begin_ts_1, end_ts_1, begin_ts_2, end_ts_2):
        """
        Initializes the MobilitydbLayerHandler with the given parameters.
        
        Args:
            iface: Interface for interacting with the QGIS application.
            task_manager: QGIS Task manager for handling asynchronous tasks.
            database_controller: Controller for database operations.
            limit: Maximum number of records to fetch.
            WaitingTDelta: Object managing waiting state for handling background threads.
            start_ts: Start timestamp for animation.
            begin_ts: Beginning timestamp of the current time delta.
            end_ts: End timestamp of the current time delta.
            next_begin_ts: Beginning timestamp of the next time delta.
            next_end_ts: End timestamp of the next time delta.
        """
        self.iface = iface
        self.task_manager = task_manager
        self.database_controller = database_controller
        self.limit = limit
        self.WaitingTDelta = WaitingTDelta
        self.WaitingTDelta.set_waiting(True)

        self.vlayer_created = False
        self.previous_tpoints = None
        self.current_tpoints = None
        self.next_tpoints = None
        
        self.geometries = {}
        self.id = 0
        self.start_ts = start_ts

        self.begin_ts = begin_ts_1
        self.end_ts = end_ts_1
        self.next_begin_ts = begin_ts_2
        self.next_end_ts = end_ts_2

        self.last_time_record = time.time()
        task = InitializeFeaturesTask("Fetching MobilityDB Data", "MobilityDB Data", self.database_controller, self.create_vector_layer, self.raise_error)
        self.task_manager.addTask(task)
 

    def reset_layer(self) -> None:
        """
        Resets the current layer, clearing visible geometries and resetting the waiting state.
        """
        self.current_tpoints = None
        self.next_tpoints = None
        self.previous_tpoints = None
        self.WaitingTDelta.set_waiting(False)   

        if len(self.geometries) > 0:
            empty_geom = Point().wkb
            for i in range(1, self.objects_count + 1):
                self.geometries[i].fromWkb(empty_geom)
            
            self.vector_layer_controller.vlayer.startEditing()
            self.vector_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries)
            self.vector_layer_controller.vlayer.commitChanges()

    def reload_animation(self, begin_ts_1, end_ts_1, begin_ts_2, end_ts_2) -> None:
        """
        Reloads the animation with new time deltas.

        Args:
            begin_ts_1: New beginning timestamp of the current time delta.
            end_ts_1: New end timestamp of the current time delta.
            begin_ts_2: New beginning timestamp of the next time delta.
            end_ts_2: New end timestamp of the next time delta.
        """
        self.current_tpoints = None
        self.next_tpoints = None
        self.next_begin_ts = begin_ts_2
        self.next_end_ts = end_ts_2

        self.last_time_record = time.time()
        self.task = FetchTimeDeltaTask(f"Fetching Time Delta", "Move - MobilityDB", self.database_controller, begin_ts_1, end_ts_1, self.limit, self.id, self.fetch_second_time_delta, self.raise_error)
        self.task_manager.addTask(self.task)

    def fetch_second_time_delta(self, result_params: dict) -> None:
        """
        Handles the result of fetching the second time delta.

        Args:
            result_params: Dictionary containing result parameters including 'TgeomPoints_list'.
        """
        try:
            self.TIME_time_delta_fetch = time.time() - self.last_time_record
            log(f"Time taken to fetch time delta TgeomPoints : {self.TIME_time_delta_fetch}")

            self.current_tpoints = result_params['TgeomPoints_list']
            self.new_frame(self.start_ts)            
            self.fetch_time_delta(self.next_begin_ts, self.next_end_ts)
            self.WaitingTDelta.set_waiting(False)
            iface.messageBar().pushMessage("Info", "Vector layer created, first time delta loaded, animation can play", level=Qgis.Info)
            log("Vector layer created, first time delta loaded, animation can play")
        except Exception as e:
            log(f"Error in fetch_second_time_delta : {e}")

    def fetch_time_delta(self, begin_ts, end_ts) -> None:
        """
        Fetch the TgeomPoints for the given time delta.

        Args:
            begin_ts: Beginning timestamp of the time delta.
            end_ts: End timestamp of the time delta.
        """
        self.last_time_record = time.time()
        self.task = FetchTimeDeltaTask(f"Fetching Time Delta", "Move - MobilityDB", self.database_controller, begin_ts, end_ts, self.limit, self.id, self.on_fetch_time_delta_finished, self.raise_error)
        self.task_manager.addTask(self.task)


    def on_fetch_time_delta_finished(self, result_params: dict) -> None:
        """
        Callback function for the fetch data task.

        Args:
            result_params: Dictionary containing result parameters including 'TgeomPoints_list'.
        """
        try:
            self.TIME_fetch_time_delta = time.time() - self.last_time_record
            if self.id == result_params['id']:
                log(f"Time taken to fetch time delta TgeomPoints : {self.TIME_fetch_time_delta}")

                if self.WaitingTDelta.is_waiting():
                    iface.messageBar().pushMessage("Info", "Data loaded, restarting animation", level=Qgis.Info)
                    self.WaitingTDelta.set_waiting(False)
                    self.previous_tpoints = self.current_tpoints
                    self.current_tpoints = result_params['TgeomPoints_list']
                    self.next_tpoints = None
                    self.temporal_controller.playForward()
                else:
                    log("Next time delta is ready")
                    self.next_tpoints = result_params['TgeomPoints_list']
            else:
                log("Thread from previous configuration terminated")
            
        except Exception as e:
            log(f"Error in on_fetch_data_finished : {e}")

    def raise_error(self, msg: str) -> None:
        """
        Function called when the task to fetch the data from the MobilityDB database failed.

        Args:
            msg: Error message.
        """
        if msg:
            log("Error: " + msg)
        else:
            log("Unknown error")

    def switch_time_delta(self) -> None:
        """
        Switches to the next time delta if the script is not waiting for a background task to end.
        """
        if not self.WaitingTDelta.is_waiting():
            self.previous_tpoints = self.current_tpoints
            self.current_tpoints = self.next_tpoints
            self.next_tpoints = None
        else:
            log("Waiting for next time delta to load")



    def create_vector_layer(self, result_params: dict) -> None:
        """
        This function initiates the vector layer controller and adds features to the layer.

        Args:
            result_params: Dictionary containing result parameters including 'Ids_timestamps' and 'srid'.
        """
        self.TIME_get_ids_timestamps = time.time() - self.last_time_record
        log(f"Time taken to fetch Ids and start/end timestamps: {self.TIME_get_ids_timestamps}")
        ids_timestamps = result_params['Ids_timestamps']
        
        self.objects_count = len(ids_timestamps)
        srid = result_params['srid']
        log(f"Number of TgeomPoints fetched : {self.objects_count} | SRID : {srid}")
        
        self.vector_layer_controller = VectorLayerController(srid)

        vlayer_fields = self.vector_layer_controller.get_vlayer_fields()
        features_list = []
        self.geometries = {}

        for i in range(1, self.objects_count + 1):
            feature = QgsFeature(vlayer_fields)
            feature.setAttributes([ids_timestamps[i-1][0], QDateTime(ids_timestamps[i-1][1]), QDateTime(ids_timestamps[i-1][2])])
            geom = QgsGeometry()
            self.geometries[i] = geom
            feature.setGeometry(geom)
            features_list.append(feature)
            
        self.vector_layer_controller.add_features(features_list)
        self.vlayer_created = True

        self.last_time_record = time.time()
        self.task = FetchTimeDeltaTask(f"Fetching Time Delta", "Move - MobilityDB", self.database_controller, self.begin_ts, self.end_ts, self.limit, self.id, self.fetch_second_time_delta, self.raise_error)
        self.task_manager.addTask(self.task)

    


    def new_frame(self, timestamp) -> None:
        """
        Update the layer to the new frame.

        Args:
            timestamp: Timestamp of the new frame.
        """
        log(f"New Frame : {timestamp}")
        try:
            if self.current_tpoints:
                log(f"New Frame : {timestamp}")
                hits = 0
                
                empty_geom = Point().wkb
                for i in range(1, self.objects_count + 1):
                    try:
                        position = self.current_tpoints[i-1][0].value_at_timestamp(timestamp)
                        self.geometries[i].fromWkb(position.wkb)
                        hits += 1
                    except:
                        self.geometries[i].fromWkb(empty_geom)

                log(f"Number of hits : {hits}")
                self.vector_layer_controller.vlayer.startEditing()
                self.vector_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries)
                self.vector_layer_controller.vlayer.commitChanges()
                self.iface.vectorLayerTools().stopEditing(self.vector_layer_controller.vlayer)
                self.iface.mapCanvas().refresh()
            else:
                log("No TgeomPoints loaded yet")
        except Exception as e:
            log(f"Error in new_frame: {e} \ Make sure the current time delta exists")



class Move:
    """
    Manages the animation of time deltas in a QGIS application
    
    """

    def __init__(self) -> None:
        """
        Initializes the Move class, setting up necessary components and attributes.
        
        Attributes:
            iface: Interface for interacting with the QGIS application.
            task_manager: Manager for handling asynchronous tasks.
            canvas: Map canvas of the QGIS application.
            temporal_controller: Controller for temporal data in QGIS.
            WaitingTDelta: Object managing waiting state for time delta operations.
            frame: Current frame number.
            key: Current key/index for time delta fetching.
            next_key: Next key/index for time delta fetching.
            begin_frame: Start frame for the current time delta.
            end_frame: End frame for the current time delta.
            is_move_disabled: Flag to disable movement.
            fps_record: List of FPS records.
            onf_record: List of ONF records.
            matrix_cap_record: List of matrix capacity records.
            navigationMode: Current navigation mode of the temporal controller.
            frameDuration: Duration of a single frame.
            temporalExtents: Temporal extents of the data.
            cumulative_range: Cumulative range of temporal data.
            total_frames: Total number of frames.
            time_delta_size: Size of the time delta.
            mobilitydb_layer_handler: Handler for the MobilityDB layer.
        """
        pymeos_initialize()
        self.iface = iface
        self.task_manager = QgsTaskManager()
        self.canvas = self.iface.mapCanvas()
        self.temporal_controller = self.canvas.temporalController()
        self.WaitingTDelta = WaitingTDelta()

        # Attributes 
        self.frame = 0
        self.key = 0
        self.next_key = 0
        self.begin_frame = 0
        self.end_frame = 0
        self.is_move_disabled = False
        self.fps_record = []
        self.onf_record = []
        self.matrix_cap_record = []

        self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated 
        self.temporal_controller.setNavigationMode(QgsTemporalNavigationObject.NavigationMode.Animated)
        self.frameDuration = self.temporal_controller.frameDuration()
        self.temporalExtents = self.temporal_controller.temporalExtents()
        self.cumulative_range = self.temporal_controller.temporalRangeCumulative()
        self.total_frames = self.temporal_controller.totalFrameCount()
        self.temporal_controller.updateTemporalRange.connect(self.on_new_frame)

        # States for NavigationMode etc
        self.time_delta_size = TIME_DELTA_SIZE 
        # self.mobilitydb_layers = []
        self.execute()

    def execute(self) -> None:
        """
        Executes the initial setup for time deltas and database connection.
        """
        connection_parameters = {
                'host': DATABASE_HOST,
                'port': DATABASE_PORT,
                'dbname': DATABASE_NAME,
                'user': DATABASE_USER,
                'password': DATABASE_PASSWORD,
                'table_name': TPOINT_TABLE_NAME,
                'id_column_name': TPOINT_ID_COLUMN_NAME,
                'tpoint_column_name': TPOINT_COLUMN_NAME,
            }
        
        self.database_connector = DatabaseController(connection_parameters)
      
        time_range = QgsDateTimeRange(self.database_connector.get_min_timestamp(), self.database_connector.get_max_timestamp())
        start_ts = self.temporal_controller.dateTimeRangeForFrameNumber(0).begin().toPyDateTime()
        self.temporal_controller.setTemporalExtents(time_range)

        self.key = 0
        
        begin_frame = self.key*self.time_delta_size
        end_frame = (begin_frame + self.time_delta_size) - 1
        
        begin_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        
        self.begin_frame = begin_frame + self.time_delta_size
        self.end_frame = end_frame + self.time_delta_size

        begin_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.begin_frame).begin().toPyDateTime()
        end_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.end_frame).begin().toPyDateTime()
        
        self.next_key = self.key + 1

        self.mobilitydb_layer_handler = MobilitydbLayerHandler(self.iface, self.task_manager, self.database_connector, LIMIT, self.WaitingTDelta, start_ts, begin_ts_1, end_ts_1, begin_ts_2, end_ts_2)

        


    def launch_animation(self, key: int = 0) -> None:
        """
        Launches the animation with the specified key.

        Args:
            key: Index/key for the time delta.
        """
        self.key = key
        
        begin_frame = self.key * self.time_delta_size
        end_frame = (begin_frame + self.time_delta_size) - 1
        
        begin_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        
        self.begin_frame = begin_frame + self.time_delta_size
        self.end_frame = end_frame + self.time_delta_size

        begin_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.begin_frame).begin().toPyDateTime()
        end_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.end_frame).begin().toPyDateTime()
        
        self.next_key = self.key + 1
        # self.mobilitydb_layer_handler.start_animation(begin_ts_1, end_ts_1, key2, begin_ts_2, end_ts_2)
        self.mobilitydb_layer_handler.reload_animation(begin_ts_1, end_ts_1, begin_ts_2, end_ts_2)
        log(f"Launching animation, first two time deltas \n First time delta : {begin_frame} to {end_frame} | {begin_ts_1} to {end_ts_1}  \n Second time delta: {self.begin_frame} to {self.end_frame} | {begin_ts_2} to {end_ts_2}")


    def save_fps(self) -> None:
        """
        Saves the FPS, ONF, and matrix capacity records to pickle files.
        """
        with open(f"timedeltas_mode_fps_record.pkl", "wb") as f:
            pickle.dump(self.fps_record, f)
        
        with open(f"timedeltas_mode_onf_record.pkl", "wb") as f:
            pickle.dump(self.onf_record, f)
        
        with open(f"laptop_timedeltas_mode_matrix_cap_record.pkl", "wb") as f:
            pickle.dump(self.matrix_cap_record, f)

        print("FPS saved")

    def update_layers(self, current_frame: int) -> None:
        """
        Updates the layers based on the current frame.

        Args:
            current_frame: The current frame number to update the layers for.
        """
        try:
            # Update geometries for the current frame
            self.mobilitydb_layer_handler.new_frame(
                self.temporal_controller.dateTimeRangeForFrameNumber(current_frame).begin().toPyDateTime()
            )

            # Calculate FPS and update records
            fps = 1 / (time.time() - self.onf_time)
            self.onf_record.append(fps)

            # Calculate matrix FPS capacity and update records
            matrix_fps_cap = self.time_delta_size / self.mobilitydb_layer_handler.TIME_fetch_time_delta
            self.matrix_cap_record.append(matrix_fps_cap)

            # Determine the final FPS to set for the temporal controller
            final_fps = min(fps, matrix_fps_cap)
            self.temporal_controller.setFramesPerSecond(final_fps)
            self.fps_record.append(final_fps)

            log(f"FPS: {final_fps} | ONF FPS: {fps} | Matrix FPS: {matrix_fps_cap}")

        except Exception as e:
            log(f"Error in updating layers: {e}")

    def switch_time_deltas(self) -> None:
        """
        Switches between the current and next time deltas in the MobilityDB layer handler.
        """
        self.mobilitydb_layer_handler.switch_time_delta()
        
      

    def fetch_next_time_deltas(self, begin_frame: int, end_frame: int) -> None:
        """
        Fetches the next set of time deltas based on the provided frame range.

        Args:
            begin_frame: The beginning frame number for fetching time deltas.
            end_frame: The ending frame number for fetching time deltas.
        """
        begin_ts = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        self.mobilitydb_layer_handler.fetch_time_delta(begin_ts, end_ts)

    def on_new_frame(self) -> None:
        self.onf_time = time.time()
        next_frame= self.frame + 1
        previous_frame= self.frame - 1

        current_frame = self.temporal_controller.currentFrameNumber()
        is_forward = (current_frame == next_frame)
        is_backward = (current_frame == previous_frame)
        # log(f"$$New signal variables :\nCurrent Frame : {current_frame} | Next Frame : {next_frame} | Previous Frame : {previous_frame} | Forward : {is_forward} | Backward : {is_backward} ")
        
        if is_forward:
            log("Forward signal")
            if not self.WaitingTDelta.is_waiting():
                self.frame = current_frame
                key = self.frame // self.time_delta_size

                if key == self.next_key and ((self.end_frame + self.time_delta_size) < self.total_frames): # Fetch Next time delta
                    if self.task_manager.countActiveTasks() != 0:
                        iface.messageBar().pushMessage("Info", "Animation paused, waiting for next time delta to load", level=Qgis.Info)
                        self.WaitingTDelta.set_waiting(True)
                        self.temporal_controller.pause()
                    
                    self.key = key
                    self.next_key = self.key + 1
                    self.switch_time_deltas()                        
                    self.begin_frame = self.begin_frame + self.time_delta_size
                    self.end_frame = self.end_frame + self.time_delta_size
                    self.fetch_next_time_deltas(self.begin_frame, self.end_frame)
                    log(f"next time delta : {self.begin_frame} to {self.end_frame} | self.key : {self.key} ")
                self.update_layers(self.frame)
            else:
                log("Waiting for next time delta to load")
                self.temporal_controller.pause()
                self.temporal_controller.setCurrentFrameNumber(self.frame)
        elif is_backward:
            log("Backward navigation only in the same time delta")
            key = current_frame // self.time_delta_size
            if key == self.key:
                self.frame = current_frame
                self.update_layers(self.frame)
            else:
                log("Different time delta")
                self.temporal_controller.pause()
                self.temporal_controller.setCurrentFrameNumber(self.frame)
        else:    
            
            self.temporal_controller.pause()
            self.temporal_controller.setCurrentFrameNumber(self.frame)
            self.temporal_controller_state_change_signal()

    def temporal_controller_state_change_signal(self) -> None:
        """
        Handle the signal emitted when the temporal controller settings are changed.
        """

        is_configuration_changed = False

        if self.temporal_controller.navigationMode() != self.navigationMode: # Navigation Mode change 
            is_configuration_changed = True
            if self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Animated:
                self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated
                log("Navigation Mode Animated")
            elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Disabled:
                self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Disabled
                log("Navigation Mode Disabled")
            elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Movie:
                self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Movie
                log("Navigation Mode Movie")
            elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.FixedRange:
                self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated
                log("Navigation Mode FixedRange")

        elif self.temporal_controller.frameDuration() != self.frameDuration: # Frame duration change 
            is_configuration_changed = True
            log(f"Frame duration has changed from {self.frameDuration} with {self.total_frames} frames")
            self.frameDuration = self.temporal_controller.frameDuration()
            self.total_frames = self.temporal_controller.totalFrameCount()
            log(f"to {self.frameDuration} with {self.total_frames} frames")

        
        elif self.temporal_controller.temporalExtents() != self.temporalExtents:
            is_configuration_changed = True
            log(f"temporal extents have changed from {self.temporalExtents} with {self.total_frames} frames")
            self.temporalExtents = self.temporal_controller.temporalExtents()
            self.total_frames = self.temporal_controller.totalFrameCount()
            log(f"to {self.temporalExtents} with {self.total_frames} frames")   

        elif self.temporal_controller.temporalRangeCumulative() != self.cumulative_range:
            is_configuration_changed = True
            log(f"cumulative range has changed from {self.cumulative_range}")
            self.cumulative_range = self.temporal_controller.temporalRangeCumulative()
            log(f"to {self.cumulative_range} with {self.total_frames} frames")
        
        if not is_configuration_changed:
            """
            A frame was skipped, resetting the frame to the previous frame 
            """

            log("Timeline skipped or other signal")
            self.temporal_controller.pause()
            self.temporal_controller.setCurrentFrameNumber(self.frame)
        else:
            log("Temporal controller configuration has changed, reload required")

          

    def reload_button(self) -> None:
        """
        Reloads the animation with the current temporal controller settings.
        """
        self.mobilitydb_layer_handler.id+=1

        self.frame = 0
        self.key = 0
        self.next_key = 0
        self.begin_frame = 0
        self.end_frame = 0
        self.fps_record = []
        self.onf_record = []
        self.matrix_cap_record = []

        self.mobilitydb_layer_handler.reset_layer()
        self.frameDuration = self.temporal_controller.frameDuration()
        self.temporalExtents = self.temporal_controller.temporalExtents()
        self.cumulative_range = self.temporal_controller.temporalRangeCumulative()
        self.total_frames = self.temporal_controller.totalFrameCount()
        self.temporal_controller.setCurrentFrameNumber(0)
        self.temporal_controller.pause()
   
        self.launch_animation(self.key)
        
move = Move()