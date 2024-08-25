"""
Interactive Mode Script

This script is designed to be executed within QGIS's Python console.
Before running, update the database connection parameters as needed.

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
)
from PyQt5.QtCore import QDateTime

# DATABASE PARAMETERS

DATABASE_USER = ""
DATABASE_PASSWORD = ""
DATABASE_HOST = ""
DATABASE_PORT = ""
SRID = 0
DATABASE_NAME = ""
TPOINT_TABLE_NAME = ""
TPOINT_ID_COLUMN_NAME = ""
TPOINT_COLUMN_NAME = ""

LIMIT = 500 

AUTOMATIC_FPS_UPDATE = False


def log(message: str) -> None:
    """
    Logs a message to the QGIS message log under the 'Move' tab with an info level.

    :param message: The message to log.
    """
    QgsMessageLog.logMessage(message, 'Move', level=Qgis.Info)


class DatabaseController:
    """
    Singleton class to handle the MobilityDB connection.
    """
    def __init__(self, connection_parameters: dict):
        """
        Initialize the DatabaseController with connection parameters.

        :param connection_parameters: A dictionary containing database connection parameters.
        """
        try:
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

        except KeyError as e:
            log(f"Missing connection parameter: {e}")
            raise

    def get_TgeomPoints(self) -> list:
        """
        Fetch TgeomPoints from the database.

        :return: A list of TgeomPoints fetched from the database.
        """
        try:
            query = (
                f"SELECT {self.id_column_name}, {self.tpoint_column_name}, "
                f"startTimestamp({self.tpoint_column_name}), "
                f"endTimestamp({self.tpoint_column_name}) "
                f"FROM public.{self.table_name} LIMIT {LIMIT};"
            )
            log(f"Executing query: {query}")

            self.connection = MobilityDB.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)

            results = []
            while True:
                rows = self.cursor.fetchmany(1000)
                if not rows:
                    break
                results.extend(rows)

            self.cursor.close()
            self.connection.close()

            return results

        except Exception as e:
            log(f"Error in fetching TgeomPoints: {e}")
            return None


   

    

class VectorLayerController:
    """
    Controller for an in-memory vector layer to view TgeomPoints.

    This class creates and manages a in-memory vector layer within QGIS, specifically designed
    to handle Spatial Points with temporal properties.
    """

    def __init__(self, srid: int):
        """
        Initialize the VectorLayerController with a given SRID.

        :param srid: Spatial Reference Identifier (SRID) used to define the coordinate system.
        """
        # Create an in-memory vector layer with the specified SRID
        self.vlayer = QgsVectorLayer(f"Point?crs=epsg:{srid}", "MobilityDB Data", "memory")

        # Define the fields for the vector layer
        fields = [
            QgsField("id", QVariant.String),
            QgsField("start_time", QVariant.DateTime),
            QgsField("end_time", QVariant.DateTime)
        ]
        self.vlayer.dataProvider().addAttributes(fields)
        self.vlayer.updateFields()

        # Define the temporal properties of the vector layer
        temporal_properties = self.vlayer.temporalProperties()
        temporal_properties.setIsActive(True)
        temporal_properties.setMode(QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
        temporal_properties.setStartField("start_time")
        temporal_properties.setEndField("end_time")

        self.vlayer.updateFields()

        # Add the vector layer to the QGIS project
        QgsProject.instance().addMapLayer(self.vlayer)

    def get_vlayer_fields(self):
        """
        Get the fields of the vector layer.

        :return: A list of fields if the vector layer exists, otherwise None.
        """
        if self.vlayer:
            return self.vlayer.fields()
        return None

    def delete_vlayer(self):
        """
        Delete the vector layer from the QGIS project.
        """
        if self.vlayer:
            QgsProject.instance().removeMapLayer(self.vlayer.id())
            self.vlayer = None  # Set to None after deletion to avoid further use

    def add_features(self, features_list: list):
        """
        Add features to the vector layer.

        :param features_list: A list of QgsFeature objects to be added to the vector layer.
        """
        if self.vlayer:
            self.vlayer.dataProvider().addFeatures(features_list)
            self.vlayer.updateExtents()  # Update the layer's extent after adding features






class FetchDataThread(QgsTask):
    """
    A QGIS task to fetch TgeomPoints data from the database asynchronously.

    This class runs as a background task in QGIS, retrieving data from a database using
    the provided database connector, without blocking the User Interface. 
    """

    def __init__(self, description: str, project_title: str, database_connector, finished_fnc, failed_fnc):
        """
        Initialize the FetchDataThread.

        :param description: A description of the task.
        :param project_title: The title of the project for which data is being fetched.
        :param database_connector: An instance of the database connector used to fetch data.
        :param finished_fnc: A callback function to execute if the task finishes successfully.
        :param failed_fnc: A callback function to execute if the task fails.
        """
        super(FetchDataThread, self).__init__(description, QgsTask.CanCancel)
        self.project_title = project_title
        self.database_connector = database_connector
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc
        self.result_params = None
        self.error_msg = None
    
    def finished(self, result: bool) -> None:
        """
        Called when the task is finished.

        :param result: A boolean indicating if the task was successful.
        """
        if result:
            self.finished_fnc(self.result_params)
        else:
            self.failed_fnc(self.error_msg)

    def run(self) -> bool:
        """
        Runs the task to fetch TgeomPoints from the database.

        :return: True if the task is successful, False if an error occurred.
        """
        try:
            # Fetch TgeomPoints from the database
            self.result_params = {
                'TgeomPoints_list': self.database_connector.get_TgeomPoints()
            }
        except Exception as e:
            # Capture and store any error message
            self.error_msg = str(e)
            return False
        return True




class MobilitydbLayerHandler:
    """
    Initializes and handles the vector layer controller to display MobilityDB data.

    This class manages the creation and updating of an in-memory vector layer in QGIS
    that displays TgeomPoints data from a MobilityDB database. The data is fetched asynchronously
    at the start of the script and updated in real-time as the temporal controller advances.
    """

    def __init__(self, iface, task_manager, srid: int, connection_parameters: dict):
        """
        Initialize the MobilitydbLayerHandler.

        :param iface: The QGIS interface instance.
        :param task_manager: The QGIS task manager instance for managing background tasks.
        :param srid: The spatial reference ID (SRID) for the vector layer's coordinate system.
        :param connection_parameters: A dictionary containing database connection parameters.
        """
        self.iface = iface
        self.task_manager = task_manager
        self.vector_layer_controller = VectorLayerController(srid)
        self.database_controller = DatabaseController(connection_parameters)

        self.last_time_record = time.time()

        # Start a background task to fetch data from the MobilityDB database
        self.fetch_data_task = FetchDataThread(
            description="Fetching MobilityDB Data",
            project_title="Move",
            database_connector=self.database_controller,
            finished_fnc=self.on_fetch_data_finished,
            failed_fnc=self.raise_error
        )
        self.task_manager.addTask(self.fetch_data_task)

    def raise_error(self, msg: str) -> None:
        """
        Called when the task to fetch data from the MobilityDB database fails.

        :param msg: The error message.
        """
        if msg:
            log("Error: " + msg)
        else:
            log("Unknown error")

    def on_fetch_data_finished(self, result_params: dict) -> None:
        """
        Callback function that is called when the data fetch task is completed.
        This creates the QGIS Features associated to each trajecotry and adds them to the vector layer.

        :param result_params: A dictionary containing the trajectories.
        """
        try:
            self.TIME_fetch_tgeompoints = time.time() - self.last_time_record
            results = result_params.get('TgeomPoints_list', [])
            log(f"Number of results: {len(results)}")

            vlayer_fields = self.vector_layer_controller.get_vlayer_fields()

            features_list = []
            self.geometries = {}
            self.tpoints = {}
            index = 1

            for row in results:
                try:
                    self.tpoints[index] = row[1]
                    feature = QgsFeature(vlayer_fields)
                    feature.setAttributes([row[0], QDateTime(row[2]), QDateTime(row[3])])

                    geom = QgsGeometry()
                    self.geometries[index] = geom
                    feature.setGeometry(geom)
                    features_list.append(feature)
                    
                    index += 1

                except Exception as e:
                    log(f"Error creating feature: {e} \nRow: {row} \nIndex: {index}")

            # Add all features to the vector layer
            self.vector_layer_controller.add_features(features_list)
            self.objects_count = index - 1

            log(f"Time taken to fetch TgeomPoints: {self.TIME_fetch_tgeompoints}")
            log(f"Number of TgeomPoints fetched: {self.objects_count}")       

            results = None  # Free memory
            self.iface.messageBar().pushMessage("Info", "TGeomPoints have been loaded", level=Qgis.Info)

        except Exception as e:
            log(f"Error in on_fetch_data_finished: {e}")

    def new_frame(self, timestamp: QDateTime) -> None:
        """
        Update the layer with new geometries corresponding to the given timestamp.

        :param timestamp: The timestamp for which to update the layer's geometries.
        """
        log(f"New Frame: {timestamp}")
        visible_geometries = 0
        empty_geom = Point().wkb

        for i in range(1, self.objects_count + 1):
            try:
                # Fetch the position of the object at the current timestamp
                position = self.tpoints[i].value_at_timestamp(timestamp)

                # Update the geometry of the feature in the vector layer
                self.geometries[i].fromWkb(position.wkb)
                visible_geometries += 1
            except:
                # Set geometry to empty if position is not available
                self.geometries[i].fromWkb(empty_geom)

        log(f"{visible_geometries} visible geometries")

        # Update the geometries in the vector layer
        self.vector_layer_controller.vlayer.startEditing()
        self.vector_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries)
        self.vector_layer_controller.vlayer.commitChanges()

    
        



class Move:
    """
    Main class to handle the logic of the animation within QGIS.

    This class controls the temporal controller user interactions and the vector layers that display
    MobilityDB data. It manages the frames and handles changes in the temporal controller's settings.
    """

    def __init__(self):
        """
        Initialize the Move class and set up the temporal controller and other components.
        """
        pymeos_initialize()
        self.iface = iface
        self.task_manager = QgsTaskManager()
        self.canvas = self.iface.mapCanvas()
        self.temporal_controller = self.canvas.temporalController()
        self.fps_record = []

        # Set the initial navigation mode to animated
        self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated
        self.temporal_controller.setNavigationMode(self.navigationMode)

        # Retrieve initial temporal settings
        self.frameDuration = self.temporal_controller.frameDuration()
        self.temporalExtents = self.temporal_controller.temporalExtents()
        self.total_frames = self.temporal_controller.totalFrameCount()
        self.cumulativeRange = self.temporal_controller.temporalRangeCumulative()

        # Connect the signal to handle frame updates
        self.temporal_controller.updateTemporalRange.connect(self.on_new_frame)

        # Initialize frame count and layers list
        self.frame = 0
        self.mobilitydb_layer = None

        # Execute the main logic to set up the layers
        self.execute()

    def execute(self):
        """
        Set up the connection parameters and initialize the MobilityDB layers.
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

        # Initialize the MobilityDB layer handler
        self.mobilitydb_layer = MobilitydbLayerHandler(self.iface, self.task_manager, SRID, connection_parameters)

    def save_fps(self):
        """
        Save the recorded FPS to a file for later analysis.
        """
        with open("interactive_mode_fps_records.pkl", 'wb') as f:
            pickle.dump(self.fps_record, f)
        print("FPS records saved")

    def on_new_frame(self):
        """
        Handle the event when a new frame is displayed during the animation.

        This method handles the signal emitted by the temporal controller , either when a new frame 
        is displayed or when the temporal controller settings are changed.
        """
        recommended_fps_time = time.time()

        next_frame = self.frame + 1
        previous_frame = self.frame - 1
        current_frame = self.temporal_controller.currentFrameNumber()

        if current_frame == next_frame or current_frame == previous_frame:
            self.frame = current_frame
            self.mobilitydb_layer.new_frame(self.temporal_controller.dateTimeRangeForFrameNumber(current_frame).begin().toPyDateTime())
            
            fps = 1 / (time.time() - recommended_fps_time)
            self.fps_record.append(fps)
            log(f"FPS: {fps}")
            if AUTOMATIC_FPS_UPDATE:
                self.temporal_controller.setFramesPerSecond(fps)
        else:
            self.frame = current_frame
            iface.messageBar().pushMessage("Info", "Temporal Controller settings were changed", level=Qgis.Info)
            self.temporal_controller_state_change_signal()

    def temporal_controller_state_change_signal(self):
        """
        Handle the signal emitted when the temporal controller settings are changed.
        """
        if self.temporal_controller.navigationMode() != self.navigationMode: 
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
        elif self.temporal_controller.frameDuration() != self.frameDuration:
            self.frameDuration = self.temporal_controller.frameDuration()
            self.total_frames = self.temporal_controller.totalFrameCount()
            log(f"to {self.frameDuration} with {self.total_frames} frames")

        elif self.temporal_controller.temporalExtents() != self.temporalExtents:
            log(f"temporal extents have changed from {self.temporalExtents} with {self.total_frames} frames")
            self.temporalExtents = self.temporal_controller.temporalExtents()
            self.total_frames = self.temporal_controller.totalFrameCount()
            log(f"to {self.temporalExtents} with {self.total_frames} frames")   
        elif self.temporal_controller.temporalRangeCumulative() != self.cumulativeRange:
            log(f"Cumulative range has changed from {self.cumulativeRange}")
            self.cumulativeRange = self.temporal_controller.temporalRangeCumulative()
            log(f"to {self.cumulativeRange}")
        else:
            log("Unhandled signal")

                

move = Move()