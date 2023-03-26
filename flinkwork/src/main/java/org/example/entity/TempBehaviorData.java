package org.example.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 行为数据
 *
 * @Author ly
 * @Date 2023/03/26,0026,周日 下午 07:40
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TempBehaviorData implements Serializable {

    private static final long serialVersionUID = 1406585678252614023L;
    private String userId;
    private String itemId;
    private String categoryId;
    private String behavior;

    private Long timestamp;
}
