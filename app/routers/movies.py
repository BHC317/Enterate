from datetime import datetime
from fastapi import APIRouter, Body, HTTPException, Path, Request, status, Header
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from logger import log

'''
CRUD functionality with movie resources
'''

router = APIRouter(tags=["movies"], prefix="/movies")
    
class MovieSchema(BaseModel):
    title: str = Field(description="Title of the movie", min_length=1, max_length=30, pattern="^[A-Z]", example="Matrix")
    year: int = Field(description="Year of the movie", gt=1900, lt=2500, example=1999)
    producer: str = Field(description="Producer of the movie", deprecated=True, default="MGM")

class MovieModel(MovieSchema):
    creation_date: datetime
    
class HTTPExceptionModel(BaseModel):
    detail: str = Field(description="Error message", example="Movie id not found")
    
class MovieResponseModel(BaseModel):
    msg: str = Field(description="Message", example="Movie deleted")    
    
movies: dict[int, MovieModel] = {}

@router.post("/{id}",
    summary="Create new movie",
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_201_CREATED: {"description": "Movie created", "model": MovieSchema},
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid movie id", "model": HTTPExceptionModel},
        
    },
    response_model=MovieSchema
)
def new_movie(movie: MovieSchema = Body(description="New movie"), id: int = Path(description="Movie id")):    
    if id in movies:
        raise HTTPException(detail="Movie id in use", status_code=status.HTTP_400_BAD_REQUEST)
    movies[id] = MovieModel(title=movie.title, year=movie.year, producer=movie.producer, creation_date = datetime.now())
    return movies[id]

@router.get("/{id}",
    summary="Get movie",   
    responses={
        status.HTTP_200_OK: {"description": "Movie returned", "model": MovieSchema},
        status.HTTP_404_NOT_FOUND: {"description": "Movie id not found", "model": HTTPExceptionModel},
    },
    response_model=MovieSchema
)
def get_movie(request: Request, id: int = Path(description="Movie id"), my_special_header: str = Header(description = "Speacial header")):   
    headers = request.headers
    log.info(f"special-header {my_special_header} resto {headers}")
    if id not in movies:
        raise HTTPException(detail="Movie id not found", status_code=status.HTTP_404_NOT_FOUND)        
    return movies[id]

@router.get("/",
    summary="Get movie list",   
    responses={status.HTTP_200_OK: {"description": "Movie list returned", "model": list[MovieSchema]}},
    response_model=list[MovieSchema]
)
def get_movie_list():    
    return list(movies.values())

@router.delete("/{id}",
    summary="Delete movie",    
    responses={
        status.HTTP_200_OK: {"description": "Movie deleted"},
        status.HTTP_404_NOT_FOUND: {"description": "Movie id not found", "model": HTTPExceptionModel},
    },
    response_class = PlainTextResponse
)
def delete_movie(id: int = Path(description="Movie id")):
    if id not in movies:
        raise HTTPException(detail="Movie id not found", status_code=status.HTTP_404_NOT_FOUND)    
    del movies[id]
    return "Movie deleted"

@router.put("/{id}",
    summary="Put movie",    
    responses={
        status.HTTP_200_OK: {"description": "Movie updated"},
        status.HTTP_404_NOT_FOUND: {"description": "Movie id not found", "model": HTTPExceptionModel},
    },
    response_model=MovieSchema
)
def update_movie(movie: MovieSchema = Body(description="Movie to be updated"), id: int = Path(description="Movie id")):
    if id not in movies:
        raise HTTPException(detail="Movie id not found", status_code=status.HTTP_404_NOT_FOUND)    
    movies[id] = MovieModel(title=movie.title, year=movie.year, producer=movie.producer, creation_date = datetime.now())
    return movies[id]



