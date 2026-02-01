from typing import Annotated

from fastapi import APIRouter, Depends

# from api.deps import verify_api_key
from schemas.response_schemas import SuccessResponse
from services.prices.lanseti.price_loader import PriceLoader, get_price_loader


load_prices_router = APIRouter()  # dependencies=[Depends(verify_api_key)])


@load_prices_router.post(  # type: ignore[misc]
    "/load-prices", summary="Load price of supplier"
)
async def load_price(
    # supplier: Annotated[
    #    str, (..., description="supplier")
    # ],
    price_loader: Annotated[PriceLoader, Depends(get_price_loader)],
) -> SuccessResponse:
    path = await price_loader.process_price()
    return SuccessResponse(data={"path": path}, message="Price loaded")
