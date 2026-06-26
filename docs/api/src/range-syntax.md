> [!TIP]
> The `priority`, `ready_at`, and `attempts` parameters accept a range
> expression with **inclusive** bounds on both ends. Four shapes are
> supported:
>
> | Shape | Meaning |
> | --- | --- |
> | `N` | Exactly `N` (sugar for `N..N`). |
> | `A..B` | Between `A` and `B`. |
> | `..B` | At most `B`. |
> | `A..` | At least `A`. |
>
> Requests where the lower bound exceeds the upper bound are rejected with
> `400 Bad Request`.
