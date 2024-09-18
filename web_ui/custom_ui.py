from typing_extensions import Self
from nicegui import ui
from nicegui.binding import BindableProperty, bind_from
from typing import cast
from datetime import datetime as dt
from itertools import groupby
from typing import Callable
from icecream import ic
from shared.config import *

# TODO: add humanize
date_to_str = lambda date: dt.fromtimestamp(date).strftime('%a, %b %d')
rounded_number = lambda counter: str(counter) if counter < 100 else str(99)+'+'
rounded_number_with_max = lambda counter, top: str(counter) if counter <= top else str(top)+'+'

def groupby_date(items: list, date_field):
    flatten_date = lambda item: dt.fromtimestamp(date_field(item)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    return {date_val: list(group_items) for date_val, group_items in groupby(items, flatten_date)}

class BindableTimeline(ui.timeline):
    items = BindableProperty(on_change=lambda sender, value: cast(Self, sender)._render())

    def __init__(self, item_render_func, items: list|dict = None, date_field = lambda x: x['date'], header_field: str = lambda x: x["name"], groupby_time: bool = False):
        super().__init__()
        self.items = items
        self.date_field = date_field
        self.header_field = header_field
        self.render_item = item_render_func
        self.groupby_time = groupby_time
        self._render()
    
    def _render(self):    
        self.clear()
        items = self.items or []           
        with self:
            if not self.groupby_time:
                for item in items:
                    date = self.date_field(item)
                    with ui.timeline_entry(
                        title=self.header_field(item), 
                        subtitle=(date_to_str(date) if isinstance(date, (int, float)) else date)):
                        self.render_item(item)
            else:                
                for date, group in groupby_date(items, self.date_field).items():
                    with ui.timeline_entry(
                        title=", ".join(self.header_field(item) for item in group), 
                        subtitle=(date_to_str(date) if isinstance(date, (int, float)) else date)):
                        for item in group:
                            self.render_item(item)

    def bind_items_from(self, target_object, target_name: str = 'items', backward = lambda x: x) -> Self:
        bind_from(self, "items", target_object, target_name, backward)
        return self

class BindableList(ui.list):
    items = BindableProperty(on_change=lambda sender, value: cast(Self, sender)._render(value))

    def __init__(self, item_render_func, items: list = None):
        super().__init__()
        self.items = items or []
        self.render_item = item_render_func
        self._render(self.items)
    
    def _render(self, value):    
        self.clear()    
        with self:
            for item in (value or []):                                    
                self.render_item(item)

    def bind_items_from(self, target_object, target_name: str = 'items', backward = lambda x: x) -> Self:
        bind_from(self, "items", target_object, target_name, backward)
        return self
    
class BindableNavigationMenu(ui.menu):
    items = BindableProperty(on_change=lambda sender, value: cast(Self, sender)._render(value))

    def __init__(self, data_extraction_func, items: list = None):
        super().__init__()
        self.items = items or []
        self.extract = data_extraction_func
        self._render(self.items)
    
    def _render(self, value):    
        self.clear()    
        with self:
            for item in (value or []): 
                text, target = self.extract(item)
                ui.menu_item(text = text, on_click=target)

    def bind_items_from(self, target_object, target_name: str = 'items', backward = lambda x: x) -> Self:
        bind_from(self, "items", target_object, target_name, backward)
        return self

class BindableGrid(ui.grid):
    items = BindableProperty(
        on_change=lambda sender, value: cast(Self, sender)._render(value)
    )

    def __init__(self, item_render_func, items: list = None, rows: int = None, columns: int = None):     
        super().__init__(rows = rows, columns = columns)             
        self.items = items
        self.render_item = item_render_func    
        self._render(self.items)
   
    def _render(self, value):  
        self.clear() 
        with self:            
            for item in (value or []):
                self.render_item(item)
        self.update()

    def bind_items_from(self, target_object, target_name: str = 'items', backward = lambda x: x) -> Self:
        bind_from(self, "items", target_object, target_name, backward)
        return self

class HighlightableItem(ui.item):
    highlight = BindableProperty(
        on_change=lambda sender, value: cast(Self, sender)._render()
    )

    def __init__(self, highlight_style: str, highlight: bool = False, **kwargs):               
        self.highlight = highlight
        self.highlight_style = highlight_style
        super().__init__(**kwargs)        
        self._render()
   
    def _render(self):  
        self.style(add=self.highlight_style) if self.highlight else self.style(remove=self.highlight_style)  

    def bind_highlight_from(self, target_object, target_name: str = 'highlight', backward = lambda x: x) -> Self:
        bind_from(self, "highlight", target_object, target_name, backward)
        return self
    
class ExpandButton(ui.button):
    def __init__(self, icon="arrow_drop_down", expanded_icon="arrow_drop_up", *args, **kwargs):        
        self.value = False
        self.expanded_icon = expanded_icon
        self.unexpanded_icon = icon
        super().__init__(*args, **kwargs)
        self.props("icon-right="+self.unexpanded_icon).on_click(self.toggle)
    
    def toggle(self):
        self.value = not self.value
        self.props("icon-right="+(self.expanded_icon if self.value else self.unexpanded_icon))
      
    
_page_count = lambda item_count: min(MAX_PAGES, -(-item_count//MAX_ITEMS_PER_PAGE)) if item_count else 0

class BindablePagination(ui.pagination):
    item_count = BindableProperty(on_change=lambda sender, value: cast(Self, sender)._render(value))

    def __init__(self, item_count: int, *args, **kwargs):
        self.item_count = item_count
        super().__init__(1, _page_count(item_count), *args, **kwargs)
        self.bind_visibility_from(self, "item_count", lambda x: (x > MAX_ITEMS_PER_PAGE) if isinstance(x, int) else False)

    def _render(self, count):  
        self._props['max'] = _page_count(count)
        self.update()  

    def bind_item_count_from(self, target_object, target_name: str = 'item_count', backward = lambda x: x) -> Self:
        bind_from(self, "item_count", target_object, target_name, backward)
        return self
    
class BindablePaginatedList(ui.column):
    item_render_func: Callable
    contents = BindableProperty(on_change=lambda sender, value: cast(Self, sender)._render(value))
    _get_page: Callable
    _item_count: int
    _banner: str
    _page_index: int
    _items: list    

    def __init__(self, item_render_func: Callable):
        self.item_render_func = item_render_func        
        super().__init__()
        self._render((None, None, None)) 

    def bind_contents_from(self, target_object, target_name: str = 'contents', backward = lambda x: x) -> Self:
        bind_from(self, "contents", target_object, target_name, backward)
        return self
    
    def _load_new_page(self):
        self._items = self._get_page((self._page_index-1)*MAX_ITEMS_PER_PAGE, MAX_ITEMS_PER_PAGE)

    def _render(self, value):
        self.clear()

        # unpack value
        self._get_page, self._item_count, self._banner = value
        if not self._get_page:
            return
        
        self._page_index, self._items = 1, self._get_page(0, MAX_ITEMS_PER_PAGE)
        with self:
            ui.label().bind_text_from(self, '_banner').bind_visibility_from(self, '_banner').classes("text-h5")
            BindablePagination(self._item_count, direction_links=True, on_change=self._load_new_page).bind_value(self, "_page_index").bind_item_count_from(self, '_item_count')
            BindableList(item_render_func=self.item_render_func).bind_items_from(self, "_items")
            BindablePagination(self._item_count, direction_links=True, on_change=self._load_new_page).bind_value(self, "_page_index").bind_item_count_from(self, '_item_count')
        
    


