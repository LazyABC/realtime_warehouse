package com.lazy.realtime.ads.bean.format;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 19:07:16
 * @Details:
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeriesBean<T> {

    private String name;
    private List<T> data;
}
