from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import DataRequest, DataRequestStatus, DataRequestVote, Proposal, User, UserRole
from app.utils.ids import generate_id


class DataRequestService:
    def __init__(self, db: Session):
        self.db = db

    def create_request(
        self,
        actor_user_id: str,
        title: str,
        description: str,
        desired_columns: str | None = None,
        showcase_proposal_id: int | None = None,
    ) -> DataRequest:
        actor = self._get_user_or_raise(actor_user_id)

        if showcase_proposal_id is not None:
            proposal = self.db.execute(
                select(Proposal).where(Proposal.id == showcase_proposal_id)
            ).scalar_one_or_none()
            if proposal is None:
                raise ValueError(f"Proposal not found: {showcase_proposal_id}")
            if not proposal.is_showcase:
                raise ValueError("指定された提案はショーケース事例ではありません")

        request_id = self._next_request_id()
        data_request = DataRequest(
            request_id=request_id,
            user_id=actor.id,
            title=title,
            description=description,
            desired_columns=desired_columns,
            showcase_proposal_id=showcase_proposal_id,
        )
        self.db.add(data_request)
        self.db.commit()
        self.db.refresh(data_request)
        return data_request

    def list_requests(self, status_filter: str | None = None) -> list[DataRequest]:
        stmt = select(DataRequest)
        if status_filter:
            stmt = stmt.where(DataRequest.status == DataRequestStatus(status_filter))
        stmt = stmt.order_by(DataRequest.vote_count.desc(), DataRequest.created_at.desc())
        return list(self.db.execute(stmt).scalars().all())

    def vote(self, request_id: str, actor_user_id: str) -> DataRequest:
        actor = self._get_user_or_raise(actor_user_id)
        data_request = self._get_request_or_raise(request_id)

        existing_vote = self.db.execute(
            select(DataRequestVote).where(
                DataRequestVote.request_id == data_request.id,
                DataRequestVote.user_id == actor.id,
            )
        ).scalar_one_or_none()

        if existing_vote:
            self.db.delete(existing_vote)
            data_request.vote_count = max(0, data_request.vote_count - 1)
        else:
            vote = DataRequestVote(request_id=data_request.id, user_id=actor.id)
            self.db.add(vote)
            data_request.vote_count += 1

        self.db.commit()
        self.db.refresh(data_request)
        return data_request

    def update_status(self, request_id: str, actor_user_id: str, new_status: str) -> DataRequest:
        actor = self._get_user_or_raise(actor_user_id)
        if actor.role not in (UserRole.hr, UserRole.data_owner):
            raise PermissionError("Only hr/data_owner users can update request status")

        data_request = self._get_request_or_raise(request_id)
        data_request.status = DataRequestStatus(new_status)
        self.db.commit()
        self.db.refresh(data_request)
        return data_request

    def _get_user_or_raise(self, user_id: str) -> User:
        user = self.db.execute(select(User).where(User.user_id == user_id)).scalar_one_or_none()
        if user is None:
            raise ValueError(f"User not found: {user_id}")
        return user

    def _get_request_or_raise(self, request_id: str) -> DataRequest:
        data_request = self.db.execute(
            select(DataRequest).where(DataRequest.request_id == request_id)
        ).scalar_one_or_none()
        if data_request is None:
            raise ValueError(f"DataRequest not found: {request_id}")
        return data_request

    def _next_request_id(self) -> str:
        max_seq = self.db.execute(select(func.max(DataRequest.id))).scalar() or 0
        return generate_id("REQ", max_seq + 1)
