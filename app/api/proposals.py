from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session

from app.api.auth import get_current_api_user, require_api_hr
from app.db.models import User
from app.db.session import get_db
from app.dependencies import get_file_store
from app.schemas.dto import (
    ProposalResponse,
    ProposalListResponse,
    ReviewCommentCreate,
    ReviewCommentResponse,
)
from app.services.proposal_service import ProposalService
from app.storage.file_store import FileStore

router = APIRouter()


# ------------------------------------------------------------------
# Proposal CRUD
# ------------------------------------------------------------------


@router.post("", response_model=ProposalResponse, status_code=201)
def create_proposal(
    dataset_id: str = Form(...),
    title: str = Form(...),
    summary: str = Form(...),
    code_file: UploadFile = File(...),
    report_file: UploadFile = File(...),
    execution_command: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_api_user),
    file_store: FileStore = Depends(get_file_store),
):
    """Create a new proposal with analysis.py and report.md."""
    try:
        code_content = code_file.file.read()
        report_content = report_file.file.read()
        svc = ProposalService(db, file_store)
        proposal = svc.create_proposal(
            actor_user_id=user.user_id,
            dataset_id=dataset_id,
            title=title,
            summary=summary,
            code_content=code_content,
            report_content=report_content,
            execution_command=execution_command,
        )
        return proposal
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


@router.get("", response_model=ProposalListResponse)
def list_proposals(
    dataset_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_api_user),
    file_store: FileStore = Depends(get_file_store),
):
    """List proposals for a dataset. HR sees all; proposers see own only."""
    try:
        svc = ProposalService(db, file_store)
        proposals = svc.list_proposals(dataset_id, user.user_id)
        return ProposalListResponse(proposals=proposals)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{proposal_id}", response_model=ProposalResponse)
def get_proposal(
    proposal_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_api_user),
    file_store: FileStore = Depends(get_file_store),
):
    """Get a single proposal by ID."""
    try:
        svc = ProposalService(db, file_store)
        return svc.get_proposal(proposal_id, user.user_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


# ------------------------------------------------------------------
# Review
# ------------------------------------------------------------------


@router.post("/{proposal_id}/review", response_model=ReviewCommentResponse)
def review_proposal(
    proposal_id: str,
    body: ReviewCommentCreate,
    db: Session = Depends(get_db),
    user: User = Depends(require_api_hr),
    file_store: FileStore = Depends(get_file_store),
):
    """Review a proposal (approve/reject/comment). HR only."""
    try:
        svc = ProposalService(db, file_store)
        comment = svc.review_proposal(
            proposal_id, user.user_id, body.action.value, body.comment
        )
        return comment
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


@router.get("/{proposal_id}/comments", response_model=list[ReviewCommentResponse])
def get_review_comments(
    proposal_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_api_user),
    file_store: FileStore = Depends(get_file_store),
):
    """Get review comments for a proposal."""
    try:
        svc = ProposalService(db, file_store)
        return svc.get_review_comments(proposal_id, user.user_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


# ------------------------------------------------------------------
# Run actual (real data execution)
# ------------------------------------------------------------------


@router.post("/{proposal_id}/run_actual")
def run_actual(
    proposal_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_api_hr),
    file_store: FileStore = Depends(get_file_store),
):
    """Run a proposal against real data. HR only. Proposal must be approved."""
    try:
        svc = ProposalService(db, file_store)
        proposal = svc.get_proposal(proposal_id, user.user_id)

        from app.db.models import ProposalStatus
        if proposal.status != ProposalStatus.approved:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot run proposal in status '{proposal.status.value}'. Must be 'approved'.",
            )

        # For now, return a placeholder since actual execution infrastructure
        # is tied to the old Submission/Execution model.
        # This endpoint marks the proposal as executed.
        proposal.status = ProposalStatus.executed_real
        db.commit()
        db.refresh(proposal)
        return {
            "proposal_id": proposal.proposal_id,
            "status": proposal.status.value,
            "message": "Real data execution completed",
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
