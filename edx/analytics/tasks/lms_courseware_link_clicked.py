import logging

import luigi
import luigi.task

from collections import defaultdict
from urlparse import urlparse

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog, opaque_key_util


log = logging.getLogger(__name__)
LINK_CLICKED = 'edx.ui.lms.link_clicked'


class LMSCoursewareLinkClickedTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    Produce a data set that shows how many users clicked to a new page in each course.
    Includes whether the click was within the edX courseware or to an external site.
    """

    output_root = luigi.Parameter()

    enable_direct_output = True

    def mapper(self, line):
        value = self.get_event_and_date_string(line)

        if value is None:
            return
        event, _date_string = value

        event_type = event.get('event_type')
        if event_type is None:
            log.error("encountered event with no event_type: %s", event)
            return

        if event_type != LINK_CLICKED:
            return

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            log.error("encountered event with bad timestamp: %s", event)
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        course_id = event.get('context').get('course_id')
        if course_id is None or not opaque_key_util.is_valid_course_id(course_id):
            log.error("encountered explicit link_clicked event with invalid course_id: %s", event)
            return

        target_url = event_data.get('target_url')
        if target_url is None:
            log.error("encountered explicit link_clicked event with no target_url: %s", event)

        current_url = event_data.get('current_url')
        if target_url is None:
            log.error("encountered explicit link_clicked event with no current_url: %s", event)

        # a link is considered "internal" when it does not navigate away from the current host
        is_internal = 0
        if urlparse(target_url).netloc == urlparse(current_url).netloc:
            is_internal = 1

        yield (course_id), (eventlog.timestamp_to_datestamp(timestamp), is_internal)

    def reducer(self, key, values):
        """
        Emit the number of clicks for each course for each day with a click, and how many of the clicks
        were to external links
        """
        course_id = key
        date_to_click_count = defaultdict(int)
        date_to_external_click_count = defaultdict(int)
        for val in values:
            timestamp, is_internal = val
            date_to_click_count[timestamp] += 1

            if not is_internal:
                date_to_external_click_count[timestamp] += 1

        for date in date_to_click_count.keys():
            yield (course_id, date, date_to_external_click_count[date], date_to_click_count[date])

    def output(self):
        return get_target_from_url(self.output_root)

    def complete(self):
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def run(self):
        output_target = self.output()
        if not self.complete() and output_target.exists():
            output_target.remove()

        super(LMSCoursewareLinkClickedTask, self).run()
