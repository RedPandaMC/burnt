"""
XML-based Kanban Ticketing System for multi-LLM collaboration.

This module provides:
- Ticket creation, claim, release, and status management
- XML persistence and serialization
- Board status visualization
- Multi-LLM coordination through ticket assignment
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
from enum import Enum


class Column(Enum):
    BACKLOG = "backlog"
    IN_PROGRESS = "in_progress"
    REVIEW = "review"
    DONE = "done"


class TicketStatus(Enum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    REVIEW = "review"
    DONE = "done"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


COLUMNS_ORDER = [Column.BACKLOG, Column.IN_PROGRESS, Column.REVIEW, Column.DONE]

COLUMN_DISPLAY_NAMES = {
    Column.BACKLOG: "Backlog",
    Column.IN_PROGRESS: "In Progress",
    Column.REVIEW: "Review",
    Column.DONE: "Done",
}


@dataclass
class Ticket:
    id: str
    title: str
    description: str
    assignee: Optional[str] = None
    status: TicketStatus = TicketStatus.OPEN
    priority: int = 5
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    column: Column = Column.BACKLOG
    tags: List[str] = field(default_factory=list)
    history: List[Dict] = field(default_factory=list)
    
    def to_xml(self) -> ET.Element:
        ticket_elem = ET.Element("ticket")
        
        ET.SubElement(ticket_elem, "id").text = self.id
        ET.SubElement(ticket_elem, "title").text = self.title
        ET.SubElement(ticket_elem, "description").text = self.description
        ET.SubElement(ticket_elem, "assignee").text = self.assignee or ""
        ET.SubElement(ticket_elem, "status").text = self.status.value
        ET.SubElement(ticket_elem, "priority").text = str(self.priority)
        ET.SubElement(ticket_elem, "created_at").text = self.created_at.isoformat()
        ET.SubElement(ticket_elem, "updated_at").text = (self.updated_at or datetime.now()).isoformat()
        ET.SubElement(ticket_elem, "column").text = self.column.value
        
        if self.tags:
            tags_elem = ET.SubElement(ticket_elem, "tags")
            for tag in self.tags:
                ET.SubElement(tags_elem, "tag").text = tag
        
        if self.history:
            history_elem = ET.SubElement(ticket_elem, "history")
            for event in self.history:
                event_elem = ET.SubElement(history_elem, "event")
                ET.SubElement(event_elem, "timestamp").text = event.get("timestamp", "")
                ET.SubElement(event_elem, "action").text = event.get("action", "")
                ET.SubElement(event_elem, "actor").text = event.get("actor", "")
                if event.get("from_status"):
                    ET.SubElement(event_elem, "from_status").text = event["from_status"]
                if event.get("to_status"):
                    ET.SubElement(event_elem, "to_status").text = event["to_status"]
                if event.get("note"):
                    ET.SubElement(event_elem, "note").text = event["note"]
        
        return ticket_elem
    
    @classmethod
    def from_xml(cls, element: ET.Element) -> "Ticket":
        def get_text(el: ET.Element, tag: str, default: str = "") -> str:
            child = el.find(tag)
            return child.text if child is not None else default
        
        tags = []
        tags_elem = element.find("tags")
        if tags_elem is not None:
            tags = [t.text for t in tags_elem.findall("tag") if t.text]
        
        history = []
        history_elem = element.find("history")
        if history_elem is not None:
            for event_elem in history_elem.findall("event"):
                event = {
                    "timestamp": get_text(event_elem, "timestamp"),
                    "action": get_text(event_elem, "action"),
                    "actor": get_text(event_elem, "actor"),
                    "from_status": get_text(event_elem, "from_status"),
                    "to_status": get_text(event_elem, "to_status"),
                    "note": get_text(event_elem, "note"),
                }
                history.append(event)
        
        created_at = get_text(element, "created_at")
        updated_at = get_text(element, "updated_at")
        
        return cls(
            id=get_text(element, "id"),
            title=get_text(element, "title"),
            description=get_text(element, "description"),
            assignee=get_text(element, "assignee") or None,
            status=TicketStatus(get_text(element, "status", "open")),
            priority=int(get_text(element, "priority", "5")),
            created_at=datetime.fromisoformat(created_at) if created_at else datetime.now(),
            updated_at=datetime.fromisoformat(updated_at) if updated_at else None,
            column=Column(get_text(element, "column", "backlog")),
            tags=tags,
            history=history,
        )


@dataclass
class Assignee:
    id: str
    name: str
    type: str = "llm"
    active: bool = True
    
    def to_xml(self) -> ET.Element:
        elem = ET.Element("assignee")
        ET.SubElement(elem, "id").text = self.id
        ET.SubElement(elem, "name").text = self.name
        ET.SubElement(elem, "type").text = self.type
        ET.SubElement(elem, "active").text = str(self.active).lower()
        return elem
    
    @classmethod
    def from_xml(cls, element: ET.Element) -> "Assignee":
        return cls(
            id=element.find("id").text or "",
            name=element.find("name").text or "",
            type=element.find("type").text or "llm",
            active=(element.find("active").text or "true").lower() == "true",
        )


class TicketBoard:
    def __init__(self, name: str = "Default Board", storage_path: Optional[Path] = None):
        self.name = name
        self.storage_path = storage_path
        self.tickets: Dict[str, Ticket] = {}
        self.assignees: Dict[str, Assignee] = {}
        self.created_at = datetime.now()
        
        if storage_path and storage_path.exists():
            self.load()
    
    def create_ticket(
        self,
        ticket_id: str,
        title: str,
        description: str,
        priority: int = 5,
        tags: List[str] = None,
        actor: str = "system",
    ) -> Ticket:
        if ticket_id in self.tickets:
            raise ValueError(f"Ticket {ticket_id} already exists")
        
        ticket = Ticket(
            id=ticket_id,
            title=title,
            description=description,
            priority=priority,
            tags=tags or [],
            column=Column.BACKLOG,
            status=TicketStatus.OPEN,
        )
        
        ticket.history.append({
            "timestamp": datetime.now().isoformat(),
            "action": "created",
            "actor": actor,
            "note": f"Ticket created in {Column.BACKLOG.value}",
        })
        
        self.tickets[ticket_id] = ticket
        return ticket
    
    def claim_ticket(self, ticket_id: str, assignee_id: str, assignee_name: str = "") -> Ticket:
        if ticket_id not in self.tickets:
            raise ValueError(f"Ticket {ticket_id} not found")
        
        ticket = self.tickets[ticket_id]
        
        if ticket.assignee and ticket.assignee != assignee_id:
            raise ValueError(f"Ticket {ticket_id} is already claimed by {ticket.assignee}")
        
        if assignee_id not in self.assignees:
            self.assignees[assignee_id] = Assignee(id=assignee_id, name=assignee_name or assignee_id)
        
        old_status = ticket.status.value
        old_assignee = ticket.assignee
        
        ticket.assignee = assignee_id
        ticket.status = TicketStatus.IN_PROGRESS
        ticket.column = Column.IN_PROGRESS
        ticket.updated_at = datetime.now()
        
        ticket.history.append({
            "timestamp": datetime.now().isoformat(),
            "action": "claimed",
            "actor": assignee_id,
            "from_status": old_status,
            "to_status": TicketStatus.IN_PROGRESS.value,
            "note": f"Claimed by {assignee_name or assignee_id}",
        })
        
        return ticket
    
    def release_ticket(self, ticket_id: str, actor: str, note: str = "") -> Ticket:
        if ticket_id not in self.tickets:
            raise ValueError(f"Ticket {ticket_id} not found")
        
        ticket = self.tickets[ticket_id]
        
        old_status = ticket.status.value
        old_assignee = ticket.assignee
        
        ticket.assignee = None
        ticket.status = TicketStatus.OPEN
        ticket.column = Column.BACKLOG
        ticket.updated_at = datetime.now()
        
        ticket.history.append({
            "timestamp": datetime.now().isoformat(),
            "action": "released",
            "actor": actor,
            "from_status": old_status,
            "to_status": TicketStatus.OPEN.value,
            "note": note or f"Released by {actor}",
        })
        
        return ticket
    
    def move_ticket(self, ticket_id: str, column: Column, actor: str, note: str = "") -> Ticket:
        if ticket_id not in self.tickets:
            raise ValueError(f"Ticket {ticket_id} not found")
        
        ticket = self.tickets[ticket_id]
        
        old_column = ticket.column
        old_status = ticket.status
        
        ticket.column = column
        ticket.status = self._column_to_status(column)
        ticket.updated_at = datetime.now()
        
        ticket.history.append({
            "timestamp": datetime.now().isoformat(),
            "action": "moved",
            "actor": actor,
            "from_status": old_column.value,
            "to_status": column.value,
            "note": note or f"Moved to {column.value}",
        })
        
        return ticket
    
    def _column_to_status(self, column: Column) -> TicketStatus:
        mapping = {
            Column.BACKLOG: TicketStatus.OPEN,
            Column.IN_PROGRESS: TicketStatus.IN_PROGRESS,
            Column.REVIEW: TicketStatus.REVIEW,
            Column.DONE: TicketStatus.DONE,
        }
        return mapping.get(column, TicketStatus.OPEN)
    
    def get_tickets_by_column(self, column: Column) -> List[Ticket]:
        return [t for t in self.tickets.values() if t.column == column]
    
    def get_tickets_by_assignee(self, assignee_id: str) -> List[Ticket]:
        return [t for t in self.tickets.values() if t.assignee == assignee_id]
    
    def get_unassigned_tickets(self) -> List[Ticket]:
        return [t for t in self.tickets.values() if t.assignee is None]
    
    def get_ticket(self, ticket_id: str) -> Optional[Ticket]:
        return self.tickets.get(ticket_id)
    
    def get_work_distribution(self) -> Dict[str, List[Ticket]]:
        distribution = {}
        for ticket in self.tickets.values():
            if ticket.assignee:
                if ticket.assignee not in distribution:
                    distribution[ticket.assignee] = []
                distribution[ticket.assignee].append(ticket)
        return distribution
    
    def to_xml(self) -> ET.Element:
        root = ET.Element("ticket-board")
        
        ET.SubElement(root, "name").text = self.name
        ET.SubElement(root, "created_at").text = self.created_at.isoformat()
        
        columns_elem = ET.SubElement(root, "columns")
        for col in COLUMNS_ORDER:
            col_elem = ET.SubElement(columns_elem, "column", id=col.value)
            ET.SubElement(col_elem, "name").text = COLUMN_DISPLAY_NAMES[col]
        
        tickets_elem = ET.SubElement(root, "tickets")
        for ticket in self.tickets.values():
            tickets_elem.append(ticket.to_xml())
        
        assignees_elem = ET.SubElement(root, "assignees")
        for assignee in self.assignees.values():
            assignees_elem.append(assignee.to_xml())
        
        return root
    
    def to_xml_string(self, pretty: bool = True) -> str:
        root = self.to_xml()
        if pretty:
            self._indent(root)
        return ET.tostring(root, encoding="unicode", xml_declaration=True)
    
    def _indent(self, elem: ET.Element, level: int = 0):
        indent = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = indent + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = indent
            for child in elem:
                self._indent(child, level + 1)
            if not child.tail or not child.tail.strip():
                child.tail = indent
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = indent
    
    def save(self, path: Optional[Path] = None) -> Path:
        save_path = path or self.storage_path
        if not save_path:
            raise ValueError("No storage path specified")
        
        with open(save_path, "w") as f:
            f.write(self.to_xml_string())
        
        return save_path
    
    def load(self, path: Optional[Path] = None):
        load_path = path or self.storage_path
        if not load_path or not load_path.exists():
            return
        
        tree = ET.parse(load_path)
        root = tree.getroot()
        
        self.name = root.find("name").text or "Default Board"
        created_at = root.find("created_at").text
        self.created_at = datetime.fromisoformat(created_at) if created_at else datetime.now()
        
        self.tickets.clear()
        for ticket_elem in root.find("tickets").findall("ticket"):
            ticket = Ticket.from_xml(ticket_elem)
            self.tickets[ticket.id] = ticket
        
        self.assignees.clear()
        assignees_elem = root.find("assignees")
        if assignees_elem is not None:
            for assignee_elem in assignees_elem.findall("assignee"):
                assignee = Assignee.from_xml(assignee_elem)
                self.assignees[assignee.id] = assignee
    
    def format_board(self) -> str:
        lines = [
            f"=== KANBAN BOARD: {self.name} ===",
            f"Total Tickets: {len(self.tickets)}",
            "",
        ]
        
        for column in COLUMNS_ORDER:
            tickets = self.get_tickets_by_column(column)
            lines.append(f"## {COLUMN_DISPLAY_NAMES[column]} ({len(tickets)})")
            
            if not tickets:
                lines.append("  (empty)")
            else:
                for ticket in sorted(tickets, key=lambda t: t.priority):
                    assignee_info = f" @ {ticket.assignee}" if ticket.assignee else " (unclaimed)"
                    lines.append(f"  - [{ticket.id}] {ticket.title} (P{ticket.priority}){assignee_info}")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def format_work_distribution(self) -> str:
        distribution = self.get_work_distribution()
        
        lines = [
            "=== WORK DISTRIBUTION ===",
            "",
        ]
        
        if not distribution:
            lines.append("No tickets currently assigned.")
            return "\n".join(lines)
        
        for assignee_id, tickets in distribution.items():
            assignee = self.assignees.get(assignee_id)
            name = assignee.name if assignee else assignee_id
            
            lines.append(f"## {name} ({len(tickets)} tickets)")
            
            for ticket in tickets:
                lines.append(f"  - [{ticket.id}] {ticket.title} - {ticket.column.value}")
            
            lines.append("")
        
        return "\n".join(lines)


def create_sample_board(storage_path: Optional[Path] = None) -> TicketBoard:
    board = TicketBoard(name="Dev Team Board", storage_path=storage_path)
    
    board.create_ticket(
        ticket_id="TASK-001",
        title="Setup CI/CD pipeline",
        description="Configure GitHub Actions for automated testing and deployment",
        priority=1,
        tags=["infrastructure", "ci-cd"],
    )
    
    board.create_ticket(
        ticket_id="TASK-002",
        title="Fix login bug",
        description="Users are unable to login with SSO - investigate OAuth flow",
        priority=1,
        tags=["bug", "auth"],
    )
    
    board.create_ticket(
        ticket_id="TASK-003",
        title="Add user dashboard",
        description="Create new dashboard page with analytics widgets",
        priority=2,
        tags=["feature", "frontend"],
    )
    
    board.create_ticket(
        ticket_id="TASK-004",
        title="Optimize database queries",
        description="Profile and optimize slow queries in the reporting module",
        priority=3,
        tags=["performance", "backend"],
    )
    
    board.create_ticket(
        ticket_id="TASK-005",
        title="Write API documentation",
        description="Document all REST endpoints using OpenAPI spec",
        priority=4,
        tags=["docs"],
    )
    
    board.claim_ticket("TASK-002", "llm-agent-1", "Agent One")
    board.move_ticket("TASK-002", Column.IN_PROGRESS, "llm-agent-1", "Working on OAuth fix")
    
    board.claim_ticket("TASK-003", "llm-agent-2", "Agent Two")
    board.move_ticket("TASK-003", Column.REVIEW, "llm-agent-2", "Submitted for review")
    
    return board


if __name__ == "__main__":
    board = create_sample_board()
    print(board.format_board())
    print()
    print(board.format_work_distribution())
    print()
    print("=== XML Output ===")
    print(board.to_xml_string())