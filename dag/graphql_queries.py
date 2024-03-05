# query strings
issues_query = """
query($owner: String!, $repo: String!, $num_items: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    issues(first: $num_items, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) {
      edges {
        node {
          title
          number
          id
          url
          labels(first: 10) {
            edges {
              node {
                name
              }
            }
          }
          state
          stateReason
          closed
          body
          comments(first: 10) {
            edges {
                node {
                    body
                    author {
                        login
                    }
                }
            }
          }
          createdAt
          updatedAt
          closedAt
          author {
            login
          }
        }
      }
      pageInfo {
        endCursor
        hasNextPage
      }
    }
  }
}
"""

pulls_query = """
query($owner: String!, $repo: String!, $num_items: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(first: $num_items, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) {
      edges {
        node {
          title
          number
          id
          url
          labels(first: 10) {
            edges {
              node {
                name
              }
            }
          }
          createdAt
          updatedAt
          closedAt
          mergedAt
          author {
            login
          }
        }
      }
      pageInfo {
        endCursor
        hasNextPage
      }
    }
  }
}
"""

forks_query = """
query($owner: String!, $repo: String!, $num_items: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    forks(first: $num_items, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) {
      edges {
        node {
          owner { login }
          name
          createdAt
          updatedAt
        }
      }
      pageInfo {
        endCursor
        hasNextPage
      }
    }
  }
}
"""

commits_query = """
query($owner: String!, $repo: String!, $num_items: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    defaultBranchRef{
      target{
        ... on Commit{
          history(first:$num_items, after:$cursor){
            edges{
              node{
               ... on Commit{
                  id
                  message
                  author {
                    name
                    email
                    date
                  }
                  committer {
                    name
                    email
                    date
                  }
                  authoredDate
                  committedDate
                  additions
                  deletions
                  messageHeadline
                  messageBody
                }
              }
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
      }
    }
  }
}
"""

stargazers_query = """
query($owner: String!, $repo: String!, $num_items: Int!, $cursor: String) {
    repository(owner: $owner, name: $repo) {
        stargazers(first: $num_items, after: $cursor, orderBy: { field: STARRED_AT, direction: DESC }) {
            edges {
                starredAt
                node {
                    ... on User {
                        id
                        login
                        name
                        bio
                        company
                        createdAt
                        updatedAt
                    }
                }
            }
            pageInfo {
              endCursor
              hasNextPage
            }
        }
    }
}
"""

watchers_query = """
query($owner: String!, $repo: String!, $num_items: Int!, $cursor: String) {
    repository(owner: $owner, name: $repo) {
        watchers(first: $num_items, after: $cursor) {
            edges {
                node {
                    ... on User {
                        id
                        login
                        name
                        company
                        createdAt
                        updatedAt
                    }
                }
            }
            pageInfo {
              endCursor
              hasNextPage
            }
        }
    }
}
"""
