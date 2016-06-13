"""Test enrollment computations"""

import datetime

import luigi

from edx.analytics.tasks.lms_courseware_link_clicked import (
    LMSCoursewareLinkClickedTask,
    LINK_CLICKED
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin


class LMSCoursewareLinkClickedTaskMapTest(MapperTestMixin, InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.task_class = LMSCoursewareLinkClickedTask
        super(LMSCoursewareLinkClickedTaskMapTest, self).setUp()

        self.initialize_ids()

        self.user_id = 21
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.datestamp = "2013-12-17"

        self.event_templates = {
            'link_clicked_event': {
                "host": "test_host",
                "event_source": "server",
                "event_type": LINK_CLICKED,
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.user_id,
                },
                "time": "{0}+00:00".format(self.timestamp),
                "ip": "127.0.0.1",
                "event": {
                    "current_url": "http://example.com/",
                    "target_url": "http://another.example.com/",
                }
            }
        }
        self.default_event_template = 'link_clicked_event'

        self.expected_key = (self.course_id)

    def test_non_link_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_unparseable_enrollment_event(self):
        line = 'this is garbage but contains edx.ui.lms.link_clicked'
        self.assert_no_map_output_for(line)

    def test_link_clicked_event_count_per_course(self):
        line = self.create_event_log_line()
        self.assert_single_map_output(line, self.course_id, (self.datestamp, 0))

    def test_internal_link_clicked_event_count(self):
        line = self.create_event_log_line(event={
                                          "current_url": "http://courses.edx.org/blah",
                                          "target_url": "https://courses.edx.org/blargh"
                                          })
        self.assert_single_map_output(line, self.course_id, (self.datestamp, 1))


class LinkClickedEventMapTask(InitializeLegacyKeysMixin, unittest.TestCase):
    pass


class LinkClickedTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        self.task_class = LMSCoursewareLinkClickedTask
        super(LinkClickedTaskReducerTest, self).setUp()

        # Create the task locally, since we only need to check certain attributes
        self.create_link_clicked_task()
        self.user_id = 0
        self.course_id = 'foo/bar/baz'
        self.datestamp = "2013-12-17"
        self.reduce_key = self.course_id

    def test_no_events(self):
        self._check_output_complete_tuple([], ())

    def create_link_clicked_task(self, interval='2013-01-01'):
        """Create a task for testing purposes."""
        fake_param = luigi.DateIntervalParameter()
        self.task = LMSCoursewareLinkClickedTask(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
        )

    def test_multiple_events_for_course(self):
        inputs = [
            (self.datestamp, 0),
            (self.datestamp, 0),
            (self.datestamp, 0),
            (self.datestamp, 0),
            (self.datestamp, 1),
            (self.datestamp, 1),
            (self.datestamp, 1),
        ]
        expected = ((self.course_id, self.datestamp, 4, 7),)
        self._check_output_complete_tuple(inputs, expected)

    def test_external_events_only(self):
        inputs = [
            (self.datestamp, 0),
            (self.datestamp, 0),
            (self.datestamp, 0),
            (self.datestamp, 0),
            (self.datestamp, 0),
            (self.datestamp, 0),
            (self.datestamp, 0),
            (self.datestamp, 0),
        ]
        expected = ((self.course_id, self.datestamp, 8, 8),)
        self._check_output_complete_tuple(inputs, expected)

    def test_internal_events_only(self):
        inputs = [
            (self.datestamp, 1),
            (self.datestamp, 1),
            (self.datestamp, 1),
        ]
        expected = ((self.course_id, self.datestamp, 0, 3),)
        self._check_output_complete_tuple(inputs, expected)
