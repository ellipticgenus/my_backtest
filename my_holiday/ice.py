
from datetime import date

from holidays.calendars.gregorian import (
    JAN,
    FEB,
    MAR,
    APR,
    MAY,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    NOV,
    DEC,
    _timedelta,
)
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_TO_PREV_FRI, SUN_TO_NEXT_MON
import pandas as pd
import holidays
from commodity.commodconfig import *

class ICE(
     ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    market = "ICE"
    observed_label = "%s (observed)"
    start_year = 1999
    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, ICEStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_TO_PREV_FRI + SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day
        name = "New Year's Day"
        self._move_holiday(self._add_new_years_day(name))

        # MLK, 3rd Monday of January.
        if self._year >= 1998:
            self._add_holiday_3rd_mon_of_jan("Martin Luther King Jr. Day")

        # GOOD FRIDAY - closed every year except 1898, 1906, and 1907
        if self._year not in {1898, 1906, 1907}:
            self._add_good_friday("Good Friday")

    # WASHINGTON'S BIRTHDAY: Feb 22 (obs) until 1971, then 3rd Mon of Feb
        name = "Washington's Birthday"
        if self._year <= 1970:
            self._move_holiday(self._add_holiday_feb_22(name))
        else:
            self._add_holiday_3rd_mon_of_feb(name)

        # MEM DAY (May 30) - closed every year since 1873
        # last Mon in May since 1971
        if self._year >= 1873:
            name = "Memorial Day"
            if self._year <= 1970:
                self._move_holiday(self._add_holiday_may_30(name))
            else:
                self._add_holiday_last_mon_of_may(name)

                # JUNETEENTH: since 2022
        if self._year >= 2022:
            self._move_holiday(self._add_holiday_jun_19("Juneteenth National Independence Day"))

        # INDEPENDENCE DAY (July 4) - history suggests closed every year
        self._move_holiday(self._add_holiday_jul_4("Independence Day"))

      # LABOR DAY - first mon in Sept, since 1887
        if self._year >= 1887:
            self._add_holiday_1st_mon_of_sep("Labor Day")
        
        # THXGIVING DAY: 4th Thurs in Nov - closed every year
        self._add_holiday_4th_thu_of_nov("Thanksgiving Day")
        # Christmas Day.
        self._move_holiday(self._add_christmas_day("Christmas Day"))

        if  self._is_saturday(self._christmas_day):
            self._add_new_years_eve("New Year's Eve")

    def _populate_half_day_holidays(self):
        # %s (markets close at 1:00pm).
        early_close_label = "%s (markets close at 12:05pm)"

        # 1990-1992 early closings are covered by special holidays.
        if self._year >= 1993:
            # Day before Independence Day.
            jul_4 = (JUL, 4)
            if (
                self._is_weekday(jul_4)
                and not self._is_monday(jul_4)
                and self._year not in {1996, 2002}
            ):
                self._add_holiday_jul_3(early_close_label % "Day before Independence Day")

            # Day after Thanksgiving Day.
            self._add_holiday_1_day_past_4th_thu_of_nov(
                early_close_label % "Day after Thanksgiving Day"
            )

            # Christmas Eve.
            if self._is_weekday(self._christmas_day) and not self._is_monday(self._christmas_day):
                self._add_christmas_eve(early_close_label % "Christmas Eve")

class ICEStaticHolidays:
    """
    copied from nyse.
    """

    # Friday after Christmas Day.
    name_friday_after_christmas = "Friday after Christmas Day"
    # Christmas Eve.
    name_christmas_eve = "Christmas Eve"

    # %s (markets close at 1:00pm).
    close_1pm_label = "%s (markets close at 1:00pm)"
    
    # Closed for Sept 11, 2001 Attacks.
    name_sept11_attacks = "Closed for Sept 11, 2001 Attacks"

    # Day after Independence Day.
    name_day_after_independence_day = "Day after Independence Day"
    # Hurricane Sandy.
    name_hurricane_sandy = "Hurricane Sandy"

    special_public_holidays = {
     
        1999: (DEC, 31, "New Year's Eve"),

        2001: (
            (SEP, 11, name_sept11_attacks),
            (SEP, 12, name_sept11_attacks)
        ),
     
        2012: (
            (OCT, 29, name_hurricane_sandy),
            (OCT, 30, name_hurricane_sandy),
        ),

        2004: (DEC, 31, "New Year's Eve"),

    }
    special_half_day_holidays = {

        2002: (JUL, 5, close_1pm_label % name_day_after_independence_day),
        2003: (DEC, 26, close_1pm_label % name_friday_after_christmas),
        2013: (JUL, 3, close_1pm_label % "Day before Independence Day"),
    }
